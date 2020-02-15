# distutils: language = c++

from libcpp.string cimport string
from libcpp.map cimport map
from libcpp cimport bool


cdef extern from 'savime_lib.h':
    enum SavimeEnumType:
        SAV_CHAR
        SAV_INT8
        SAV_INT16
        SAV_INT32
        SAV_INT64
        SAV_UINT8
        SAV_UINT16
        SAV_UINT32
        SAV_UINT64
        SAV_FLOAT
        SAV_DOUBLE
        INVALID_TYPE

    cdef cppclass SavimeType:
        SavimeEnumType type
        int length
        inline SavimeType& operator=(SavimeEnumType)
        inline bool operator ==(SavimeEnumType)
        inline bool operator !=(SavimeEnumType)
        inline bool operator ==(SavimeType)
        inline bool operator !=(SavimeType)

    cdef cppclass SavimeDataElement:
        string name
        bool is_dimension
        SavimeType type

    cdef struct QueryResultHandle:
        char*response_text
        bool is_schema
        bool successful
        map[string, int] descriptors
        map[string, string] files
        map[string, SavimeDataElement] schema

    cdef struct SavimeConn:
        int socketfd
        int clientid
        int queryid
        int message_count
        bint opened

        SavimeConn()
        string status()

    SavimeConn open_connection(int, const char*) except +
    void close_connection(SavimeConn &) except +
    QueryResultHandle execute(SavimeConn &, const char*) except +
    int read_query_block(SavimeConn &, QueryResultHandle &) except +
    void dispose_query_handle(QueryResultHandle &) except +
    void shutdown_savime(SavimeConn &) except+

    cdef int SAV_FAILURE
    cdef int SAV_SUCCESS
    cdef int SAV_NO_MORE_BLOCKS
    cdef int SAV_BLOCKS_LEFT
    cdef int SAV_ERROR_READING_BLOCKS
    cdef int SAV_ERROR_RESPONSE_BLOCKS
