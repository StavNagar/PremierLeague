from loggers.logger import get_logger


class App:
    def __init__(self, reader, etl):
        self.reader = reader
        self.etl = etl
        self.logger = get_logger(self.__class__.__name__)
    
    def up(self):
        try:
            self.logger.info("INFO: Pipeline is UP")
            msg = self.reader.read()
            self.etl.start(msg)

        except Exception as e:
            self.logger.info(f"FAIL: Pipeline failed: {e}")
        
        finally:
            self.reader.close()
