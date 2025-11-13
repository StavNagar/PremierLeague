import ssl
import json
import http.client
from urllib.parse import urlencode
from loggers.logger import get_logger


class HttpsReader:
    def __init__(self, reader_config):
        self.logger = get_logger(self.__class__.__name__)
        self.reader_config = reader_config
        self.api_key = self.reader_config["API_KEY"]
        self.host = self.reader_config["HOST"]
        self.headers = self.reader_config.get("HEADERS", {})
        self.endpoint = self.reader_config.get("ENDPOINT", "/")
        self.method = self.reader_config.get("METHOD", "GET")
        self.params = self.reader_config.get("PARAMS", None)
        self.connection = None
        self.response_data = None

    def connect(self):
        try:
            context = ssl.create_default_context()
            self.connection = http.client.HTTPSConnection(self.host, context=context)
            self.logger.info(f"SUCCESS: Connected to {self.host}")
        except Exception as e:
            self.logger.error(f"FAIL: Connection failed -> {e}")
            raise

    def read(self):
        if not self.connection:
            self.connection = self.connect()
        
        try:
            if self.params:
                self.endpoint = f"{self.endpoint}?{urlencode(self.params)}"

            self.connection.request(self.method, self.endpoint, headers=self.headers)
            response = self.connection.getresponse()
            msg = response.read().decode("utf-8")

            self.response_data = {
                "STATUS": response.status,
                "REASON": response.reason,
                "BODY": msg
            }

            if self.check_valid_msg():
                return self.load(msg)
            else:
                return None
            
        except Exception as e:
            self.logger.error(f"FAIL: Failed consume message -> {e}")

    def check_valid_msg(self):
        if not self.response_data:
            self.logger.warning("WARNING: No response to validate")
            return False
        
        return 200 <= self.response_data["STATUS"] < 300

    def load(self, msg):
        if not self.response_data:
            raise ValueError("No response data avaliable")
        
        loaded_msg = json.loads(msg)
        self.logger.info(f"SUCCESS: Message successfully loaded")
        return loaded_msg

    def close(self):
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.logger.info("SUCCESS: Connection closed")
        except Exception as e:
            self.logger.error(f"FAIL: Failed closing connection -> {e}")
