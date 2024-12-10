
import fastjsonschema
import os
import json
from typing import Callable
from .schema_exception import SchemaError

# NOTE: Slow
class schema():
    """Decorator for functions that return a standard price_history object.

    Validates the object against the schema
    """
    
    def __init__(self, filename: str):
        self.filename = filename

    def __call__(self, func) -> Callable:
        def wrapped_func(*args, **kwargs) -> dict:
            validate = SchemaUtils._get_validation_obj(self.filename)
            obj = func(*args, **kwargs)
            assert(validate(obj) == obj)
            return obj
        return wrapped_func


class SchemaUtils():
    """Various sanity checking functions
    
    """

    _price_history_schema: str = "price-history/price_history_schema.json"

    @staticmethod
    def _get_validation_obj(schema_file_name: str) -> Callable:
        """
        Converts a .json schema into a fastjsonschema validation object

        Args:
            schema_file_name (str): Location of the schema file

        Returns:
            Callable:Validation function
        """
        curr_dir = os.path.dirname(os.path.realpath(__file__)) + "/"
        absolute_path_to_schema = curr_dir + schema_file_name
        schema = None
        with open(absolute_path_to_schema) as f:
            schema = json.loads(f.read())
        return fastjsonschema.compile(schema)
    
    @staticmethod
    def validate_price_history(price_history: dict) -> bool:
        """
        Validation function for price history. Used as sanity check in various other modules.
        Schema located at well-known location defined as class member.

        Args:
            price_history (dict): Any price_history object

        Returns:
            bool:True if valid, others fails assertion

        Raises:
            AssertionError: If the fastjsonschema validation object not equal
                to the supplied price history
        """
        validate = SchemaUtils._get_validation_obj(SchemaUtils._price_history_schema)
        assert(validate(price_history) == price_history)
        return True
