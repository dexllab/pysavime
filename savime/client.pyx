# distutils: language = c++

from libc.stdint cimport *

from posix.mman cimport *
from posix.unistd cimport close
from posix.stat cimport struct_stat, fstat

from libcpp cimport nullptr

from savime.savime cimport *

from logging_utility.logger import client_logger, connection_logger
from misc.exceptions import ConnectionFailure, QueryHandleFailure, MMapFailed, SavimeSilentError
from util.data_variable import DataVariable, DataVariableBlock
import logging_utility.logging_messages as logging_messages
from misc.decorators import timer_decorator

import numpy as np

from datatype import SavimeDataTypeUtility

# noinspection PyAttributeOutsideInit
cdef class Connection:
    """
    Savime connector class. It wraps SavimeConn (cpp) object, which in turn provides an interface to connecting to
    Savime server.

    Examples:
        >>> conn = Connection('localhost', 65000)
        >>> conn.open()
        >>> conn.close()

        >>> with Connection('localhost', 65000) as conn:
        ...     pass

    Attributes:
        connection (SavimeConn): Wrapped cpp connection.
        port (int): Port where Savime server is listening.
        host (str): Host where Savime is listening.

    """

    cdef SavimeConn connection
    cdef int port
    cdef str host

    def __cinit__(self, str host, int port):
        self.host = host
        self.port = port

    def open(self):
        """
        Open a connection.

        Returns:
            None

        Raises:
            ConnectionFailure: Whether the connection is already open or could not be opened.

        """
        connection_logger.debug(logging_messages.CONN_OPEN_TRY.format(self.port))

        if self.is_connection_open:
            msg = logging_messages.CONN_OPEN_ALREADY.format(self.port)
            connection_logger.error(msg)
            raise ConnectionFailure(msg)

        self.connection = open_connection(self.port, self.host.encode('utf-8'))

        if not self.connection.opened or self.connection.socketfd == -1:
            msg = logging_messages.CONN_OPEN_UNSUCCESSFUL.format(self.port)
            connection_logger.error(msg)
            raise ConnectionFailure(msg)

        connection_logger.info(logging_messages.CONN_OPEN_SUCCESSFUL.format(self.port))

    def close(self):
        """
        Close a connection.

        Returns:
            None

        Raises:
            ConnectionFailure: Whether the connection is already closed or could not be closed.

        """
        connection_logger.debug(logging_messages.CONN_CLOSE_TRY.format(self.port))

        if self.is_connection_closed:
            msg = logging_messages.CONN_CLOSE_ALREADY.format(self.port)
            connection_logger.error(msg)
            raise ConnectionFailure(msg)

        close_connection(self.connection)

        if self.connection.socketfd == -1:
            msg =  logging_messages.CONN_CLOSE_UNSUCCESSFUL.format(self.port)
            connection_logger.error(msg)
            raise ConnectionFailure(msg)

        connection_logger.info(logging_messages.CONN_CLOSE_SUCCESSFUL.format(self.port))

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def is_connection_open(self) -> bool:
        return self.connection.opened

    @property
    def is_connection_closed(self) -> bool:
        return not self.connection.opened

# noinspection PyAttributeOutsideInit
cdef class Client:
    """
    A Cython client for Savime.

    Examples:
          >>> query = 'select(io, x, y, a);'
          >>> conn = Connection('localhost', 50000)
          >>> client = Client(conn)
          >>> client.execute(query)

    Attributes:
        _conn_wrapper (Connection): Connection wrapper.
        _query_result_handle (QueryResultHandle): Handle for query results. Given a query, the handle is in charge of
        keeping relevant information to manipulate data managed by Savime. For example, the handle keeps information
        such as response messages, file descriptors and file paths.

    """

    cdef Connection _conn_wrapper
    cdef QueryResultHandle _query_result_handle
    cdef bool _raise_silent_error
    cdef bool _created_in_a_with_statement

    def __cinit__(self, Connection conn_wrapper=None, str host=None, int port=-1, bool raise_silent_error=False):
        assert (conn_wrapper is not None) ^ (host is not None and port != -1), 'You should provide a connection ' \
                                                                               'or a host and a port.'
        assert (host is not None and port != -1), 'You have to provide both a host and a port.'

        if conn_wrapper is not None:
            self._conn_wrapper = conn_wrapper
        else:
            self._conn_wrapper = Connection(host, port)

        self._raise_silent_error = raise_silent_error
        self._created_in_a_with_statement = False

    def __enter__(self):
        self._conn_wrapper.open()
        self._created_in_a_with_statement = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn_wrapper.close()

    def shutdown(self):
        """
        Send a shutdown message to Savime server.

        Returns:
        """
        if self._conn_wrapper.is_connection_closed:
            raise Exception('You can\'t shutdown the server if this client is not connected to it.')

        if self._created_in_a_with_statement:
            raise Exception('You can\'t shutdown a server when the client was created inside a with context.')

        shutdown_savime(self._conn_wrapper.connection)

    @timer_decorator
    def execute(self, str query, bool raise_silent_error=False):
        """
        Send the query to the connected Savime server,

        Args:
            query: A query to be executed.
            raise_silent_error: Savime's handler barely consider a query unsuccessful which makes harder to this client
            knowing when to raise an exception. Pretty often when a query is not appropriately run the server returns
            a message containing the word Error. If raise_silent_error is True, when that is the case, a exception is
            raised.
        Returns:

        """

        client_logger.info(logging_messages.CLIENT_QUERY_RUN.format(query))
        self._query_result_handle = execute(self._conn_wrapper.connection, query.encode('utf-8'))

        # TODO: Check why the server returns a weird response when trying to create a dataset which
        #  does not exist.
        try:
            msg = self._query_result_handle.response_text.decode('utf-8')
        except UnicodeDecodeError as e:
            raise

        msg = logging_messages.CLIENT_QUERY_HANDLER_RESPONSE.format(msg)

        # TODO: This a smell. Improve savime code to return whether or not a query could be successfully run.
        if 'Error' in msg:
            client_logger.error(msg)
            if raise_silent_error:
                raise SavimeSilentError(msg)
        else:
            client_logger.info(msg)

        self.check_handle_status()
        return self.process_query_response()

    cdef check_handle_status(self):
        """
        Check the handle status. 

        Raises:
            QueryHandleFailure: If the query could not be executed (even with an error).        

        Returns:

        """
        cdef str msg

        if not self._query_result_handle.successful:
            msg = logging_messages.CLIENT_QUERY_HANDLER_RESPONSE.format(self._query_result_handle.response_text.decode('utf-8'))
            client_logger.error(msg)
            raise QueryHandleFailure(msg)

    @timer_decorator
    def process_query_response(self):
        """
        Process a query response.

        Returns:

        """

        cdef int ret
        cdef list data_variable_blocks = []

        if not self._query_result_handle.is_schema:
            read_query_block(self._conn_wrapper.connection, self._query_result_handle)
        else:
            while True:
                ret = read_query_block(self._conn_wrapper.connection, self._query_result_handle)

                if ret == SAV_ERROR_RESPONSE_BLOCKS:
                    msg = logging_messages.CLIENT_READ_QUERY_BLOCK.format('SAV_ERROR_RESPONSE_BLOCKS')
                    client_logger.debug(msg)
                    break
                if ret == SAV_NO_MORE_BLOCKS:
                    msg = logging_messages.CLIENT_READ_QUERY_BLOCK.format('SAV_NO_MORE_BLOCKS')
                    client_logger.debug(msg)
                    break
                if ret == SAV_ERROR_READING_BLOCKS:
                    msg = logging_messages.CLIENT_READ_QUERY_BLOCK.format('SAV_ERROR_READING_BLOCKS')
                    client_logger.debug(msg)
                    break

                try:
                    data_variables = self.process_data_block()
                    data_variable_block = DataVariableBlock(data_variables=data_variables)
                    data_variable_blocks.append(data_variable_block)
                except MMapFailed:
                    client_logger.warn(logging_messages.CLIENT_MMAP_FAILURE)

                for entry in self._query_result_handle.descriptors:
                    close(entry.second)

                self._query_result_handle.descriptors.clear()
                client_logger.debug(logging_messages.CLIENT_DVBLOCK_RETURN.format(len(data_variable_block)))

        dispose_query_handle(self._query_result_handle)

        return data_variable_blocks

    cdef process_data_block(self):
        """
        
        :return: 
        """

        cdef map[string, void*] buffer_map
        cdef string name
        cdef SavimeDataElement data_element
        cdef struct_stat data_element_stats = {}
        cdef int64_t num_elements_in_entry = 0, minimal_num_elements = 0, num_elements_in_sub_tar = 0


        for entry in self._query_result_handle.schema:
            name = entry.first
            data_element = entry.second
            fstat(self._query_result_handle.descriptors[name], &data_element_stats)

            client_logger.debug(logging_messages.CLIENT_RECEIVE_DATA_BUFFER_TRY.format(data_element_stats.st_size))

            buffer_map[name] = <void*> mmap(nullptr, <size_t>data_element_stats.st_size, PROT_READ, MAP_SHARED,
                                            self._query_result_handle.descriptors[name], 0)

            if buffer_map[name] == MAP_FAILED:
                close(self._query_result_handle.descriptors[name])
                client_logger.error(logging_messages.CLIENT_MMAP_FAILURE)
                raise MMapFailed(logging_messages.CLIENT_MMAP_FAILURE)

            if data_element.type != INVALID_TYPE:
                num_elements_in_entry = data_element_stats.st_size //\
                                        SavimeDataTypeUtility.get_size(data_element.type.type,
                                                                       length=data_element.type.length)

            if minimal_num_elements == 0  or num_elements_in_entry < minimal_num_elements:
                minimal_num_elements = num_elements_in_entry

        num_elements_in_sub_tar = minimal_num_elements

        query_result_block = []

        for entry in self._query_result_handle.schema:
            array = self.to_numpy_array(buffer_map[entry.first], num_elements_in_sub_tar, entry.second)
            block_data = DataVariable(name=entry.first.decode('utf-8'), array=array, is_dimension=entry.second.is_dimension)
            query_result_block.append(block_data)

        client_logger.debug('Block received.')

        return query_result_block


    cdef to_numpy_array(self, void* buffer, size_t num_elements_in_array, const SavimeDataElement& data_element,
                        bool copy_data=False):
        """
        
        :param buffer: Pointer to byte array returned by Savime.
        :param num_elements_in_array: The number of elements in the subtar which contains the buffer.
        :param data_element: 
        :param copy_data:
        :return: 
        """

        cdef uint8_t [:] buffer_byte_memory_view, numpy_byte_array_view

        cdef SavimeEnumType data_element_dtype = data_element.type.type
        cdef int data_element_len = data_element.type.length
        cdef size_t num_bytes

        if data_element_dtype != INVALID_TYPE:

            num_bytes = SavimeDataTypeUtility.get_size(data_element_dtype, num_elements=num_elements_in_array,
                                                       length=data_element_len)

            # Assign the source memory view to the source buffer
            buffer_byte_memory_view = <uint8_t[:num_bytes]> buffer

            if copy_data:
                array = self.copy_buffer_to_numpy(buffer_byte_memory_view)
            else:
                array = self.copy_less_buffer_to_numpy(buffer_byte_memory_view)

            # Cast and reshape the numpy array to the correct d_type
            if data_element_dtype != SAV_CHAR:
                np_dtype = SavimeDataTypeUtility.savime_primitive_to_numpy(data_element_dtype)
                np_array_view = array.view(np_dtype)
                np_array_view = np_array_view.reshape(-1, data_element_len)
            else:
                np_array_view = array.view(np.dtype(f'a{data_element_len}')).reshape(-1, 1)
                np_array_view = np_array_view.astype(str)

            array.setflags(write=False)
            np_array_view.setflags(write=False)

            return np_array_view
        else:
            return None

    cdef copy_buffer_to_numpy(self, uint8_t [:] buffer_byte_memory_view):
        cdef uint8_t [:] numpy_byte_array_view
        array = np.zeros(buffer_byte_memory_view.shape[0], dtype=np.uint8)
        numpy_byte_array_view = array
        numpy_byte_array_view[:] = buffer_byte_memory_view
        return array


    cdef copy_less_buffer_to_numpy(self, uint8_t [:] buffer_byte_memory_view):
        array = np.asarray(buffer_byte_memory_view)
        return array