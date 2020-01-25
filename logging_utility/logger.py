import logging

client_logger_name = 'Client'
client_logger_level = logging.DEBUG

connector_logger_name = 'Connector'
connector_logger_level = logging.DEBUG

timer_logger_name = 'Timer'
timer_logger_level = logging.DEBUG

formatter = logging.Formatter('%(asctime)s [%(name)s]:%(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

client_logger = logging.getLogger(client_logger_name)
client_logger.setLevel(client_logger_level)
client_logger.addHandler(stream_handler)
client_logger.propagate = False

connection_logger = logging.getLogger(connector_logger_name)
connection_logger.setLevel(connector_logger_level)
connection_logger.addHandler(stream_handler)
connection_logger.propagate = False

timer_logger = logging.getLogger(timer_logger_name)
timer_logger.setLevel(timer_logger_level)
timer_logger.addHandler(stream_handler)
timer_logger.propagate = False

