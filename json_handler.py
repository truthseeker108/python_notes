import json
import logging
import ijson
import tempfile
import shutil
import os
from dataclasses import dataclass
from typing import Optional, Dict, Any, Union
from pathlib import Path
from jsonschema import validate, ValidationError

@dataclass
class JsonOperationResult:
    """
    Data class to hold the result of JSON operations.
    
    Attributes:
        data: The JSON data involved in the operation
        success: Boolean indicating if the operation was successful
        error_message: Description of the error if any occurred
        file_path: Path of the file involved in the operation
    """
    data: Optional[Dict[str, Any]]
    success: bool
    error_message: Optional[str]
    file_path: Optional[Path]

class JsonHandler:
    """
    A class for safely handling JSON file operations with advanced features.
    """
    
    def __init__(
        self,
        base_path: Optional[Path] = None,
        schema: Optional[Dict] = None,
        encoding: str = 'utf-8',
        max_size_bytes: int = 10_000_000,
        logger: Optional[logging.Logger] = None,
        silent: bool = False
    ):
        """
        Initialize the JSON handler with default settings.
        
        Args:
            base_path: Optional base path for path traversal protection
            schema: Optional JSON schema for validation
            encoding: Default file encoding
            max_size_bytes: Maximum allowed file size in bytes
            logger: Optional logger instance
            silent: If True, suppresses logging
        """
        self.base_path = Path(base_path).resolve() if base_path else None
        self.schema = schema
        self.encoding = encoding
        self.max_size_bytes = max_size_bytes
        self.logger = logger or logging.getLogger(__name__)
        self.silent = silent

    def _log(self, level: int, message: str) -> None:
        """Internal method for logging."""
        if not self.silent:
            self.logger.log(level, message)

    def _validate_path(self, path: Path) -> Optional[str]:
        """
        Validate file path against base path and other constraints.
        Returns error message if validation fails, None if successful.
        """
        try:
            resolved_path = path.resolve()
            if self.base_path and not resolved_path.is_relative_to(self.base_path):
                return "Access denied: File outside base path"
            return None
        except Exception as e:
            return f"Path validation error: {str(e)}"

    def _validate_schema(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Validate data against schema if one is set.
        Returns error message if validation fails, None if successful.
        """
        if self.schema:
            try:
                validate(instance=data, schema=self.schema)
                return None
            except ValidationError as e:
                return f"Schema validation error: {str(e)}"
        return None

    def read(
        self,
        file_path: Union[str, Path],
        use_streaming: bool = False,
        schema: Optional[Dict] = None
    ) -> JsonOperationResult:
        """
        Read and parse a JSON file with validation and safety checks.
        
        Args:
            file_path: Path to the JSON file
            use_streaming: If True, uses streaming parser for large files
            schema: Optional schema to override class schema for this operation
        
        Returns:
            JsonOperationResult containing operation status and data
        """
        path = Path(file_path)
        
        try:
            # Validate path
            if error_msg := self._validate_path(path):
                self._log(logging.ERROR, error_msg)
                return JsonOperationResult(None, False, error_msg, path)
            
            # Basic file validation
            if not path.exists():
                error_msg = f"File not found: {path}"
                self._log(logging.ERROR, error_msg)
                return JsonOperationResult(None, False, error_msg, path)
                
            if not path.is_file():
                error_msg = f"Not a file: {path}"
                self._log(logging.ERROR, error_msg)
                return JsonOperationResult(None, False, error_msg, path)
            
            # Check file size
            if path.stat().st_size > self.max_size_bytes:
                error_msg = f"File too large: {path}"
                self._log(logging.ERROR, error_msg)
                return JsonOperationResult(None, False, error_msg, path)
            
            # Load the JSON data
            if use_streaming:
                with path.open('rb') as file:
                    data = dict(ijson.kvitems(file, ''))
            else:
                with path.open('r', encoding=self.encoding) as file:
                    data = json.load(file)
            
            # Validate against schema
            if error_msg := self._validate_schema(data):
                self._log(logging.ERROR, error_msg)
                return JsonOperationResult(None, False, error_msg, path)
            
            return JsonOperationResult(data, True, None, path)
            
        except Exception as e:
            error_msg = f"Error reading JSON: {str(e)}"
            self._log(logging.ERROR, error_msg)
            return JsonOperationResult(None, False, error_msg, path)

    def write(
        self,
        data: Dict[str, Any],
        file_path: Union[str, Path],
        indent: int = 4,
        ensure_ascii: bool = False,
        backup: bool = True,
        atomic: bool = True,
        schema: Optional[Dict] = None
    ) -> JsonOperationResult:
        """
        Write data to a JSON file with validation and safety features.
        
        Args:
            data: Dictionary to be written as JSON
            file_path: Path where to write the JSON file
            indent: Number of spaces for indentation
            ensure_ascii: If True, escapes non-ASCII characters
            backup: If True, creates a backup of existing file
            atomic: If True, writes to temporary file first
            schema: Optional schema to override class schema for this operation
        
        Returns:
            JsonOperationResult containing operation status
        """
        path = Path(file_path)
        
        try:
            # Validate path
            if error_msg := self._validate_path(path):
                self._log(logging.ERROR, error_msg)
                return JsonOperationResult(None, False, error_msg, None)
            
            # Validate against schema
            if error_msg := self._validate_schema(data):
                self._log(logging.ERROR, error_msg)
                return JsonOperationResult(None, False, error_msg, None)
            
            # Create parent directories
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Handle backup
            backup_path = None
            if backup and path.exists():
                backup_path = path.with_suffix(f"{path.suffix}.bak")
                shutil.copy2(path, backup_path)
                self._log(logging.INFO, f"Created backup at {backup_path}")
            
            try:
                # Write data
                if atomic:
                    # Create temporary file in the same directory
                    temp_fd, temp_path = tempfile.mkstemp(
                        dir=path.parent,
                        prefix=f".{path.name}.",
                        suffix='.tmp'
                    )
                    os.close(temp_fd)
                    
                    # Write to temporary file
                    temp_path = Path(temp_path)
                    with temp_path.open('w', encoding=self.encoding) as f:
                        json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)
                    
                    # Atomic rename
                    temp_path.replace(path)
                else:
                    # Direct write
                    with path.open('w', encoding=self.encoding) as f:
                        json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)
                
                self._log(logging.INFO, f"Successfully wrote JSON to {path}")
                return JsonOperationResult(data, True, None, path)
                
            except Exception as e:
                # Restore backup if write failed
                if backup and backup_path and backup_path.exists():
                    shutil.copy2(backup_path, path)
                    self._log(logging.INFO, f"Restored backup from {backup_path}")
                raise
                
            finally:
                # Clean up temporary file
                if atomic and 'temp_path' in locals() and Path(temp_path).exists():
                    try:
                        Path(temp_path).unlink()
                    except Exception as e:
                        self._log(
                            logging.WARNING,
                            f"Failed to clean up temporary file {temp_path}: {e}"
                        )
                        
        except Exception as e:
            error_msg = f"Error writing JSON: {str(e)}"
            self._log(logging.ERROR, error_msg)
            return JsonOperationResult(None, False, error_msg, None)

def test_json_handler():
    """Unit tests for JsonHandler class."""
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create temporary directory for tests
    with tempfile.TemporaryDirectory() as tmpdir:
        # Initialize handler
        handler = JsonHandler(
            base_path=Path(tmpdir),
            schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "number"}
                },
                "required": ["name"]
            }
        )
        
        # Test data
        valid_data = {"name": "John", "age": 30}
        invalid_data = {"age": "invalid"}
        
        # Test writing
        write_result = handler.write(
            valid_data,
            Path(tmpdir) / "test.json",
            backup=True,
            atomic=True
        )
        assert write_result.success
        assert write_result.file_path.exists()
        
        # Test reading
        read_result = handler.read(write_result.file_path)
        assert read_result.success
        assert read_result.data == valid_data
        
        # Test schema validation
        invalid_result = handler.write(invalid_data, Path(tmpdir) / "invalid.json")
        assert not invalid_result.success
        assert "Schema validation error" in invalid_result.error_message
        
        # Test path traversal protection
        outside_path = Path(tmpdir).parent / "outside.json"
        result = handler.write(valid_data, outside_path)
        assert not result.success
        assert "outside base path" in result.error_message

if __name__ == "__main__":
    # Example usage
    handler = JsonHandler(
        base_path=Path.cwd(),
        schema={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"}
            },
            "required": ["name"]
        }
    )
    
    # Write data
    write_result = handler.write(
        {"name": "John", "age": 30},
        "config.json",
        backup=True,
        atomic=True
    )
    
    if write_result.success:
        print(f"Successfully wrote to {write_result.file_path}")
        
        # Read data back
        read_result = handler.read(write_result.file_path)
        if read_result.success:
            print("Read data:", read_result.data)
        else:
            print("Read error:", read_result.error_message)
    else:
        print("Write error:", write_result.error_message)
