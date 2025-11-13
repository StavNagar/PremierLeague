from loggers.logger import get_logger


class ETLPipeline:
    def __init__(self, parser, transformation, writer):
        self.parser = parser
        self.transformation = transformation
        self.writer = writer
        self.logger = get_logger(self.__class__.__name__)

    def start(self, msg):
        try:
            if msg is None:
                return
            
            msg = self.parser.parse(msg)
            if msg is None:
                return 
            
            msg = self.transformation.transform(msg)
            if msg is None:
                return
            
            self.writer.write(msg)
        except Exception as e:
            self.logger.info(f"FAIL: Error in ETL pipeline: {e}")
