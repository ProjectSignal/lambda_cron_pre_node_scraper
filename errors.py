"""
Structured error taxonomy and handling for pre-node scraper Lambda processor.
Provides comprehensive error classification, structured error information, and enterprise-grade error management.
"""

import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict

from utils import get_logger


class ErrorSeverity(Enum):
    """Error severity levels for structured error handling"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for classification"""
    API_ERROR = "api_error"
    DATA_QUALITY = "data_quality"
    DATABASE_ERROR = "database_error"
    VALIDATION_ERROR = "validation_error"
    CONFIGURATION_ERROR = "configuration_error"
    NETWORK_ERROR = "network_error"
    TRANSFORMATION_ERROR = "transformation_error"
    AUTHENTICATION_ERROR = "authentication_error"
    RATE_LIMIT_ERROR = "rate_limit_error"
    TIMEOUT_ERROR = "timeout_error"
    BUSINESS_LOGIC_ERROR = "business_logic_error"
    UNKNOWN_ERROR = "unknown_error"


class ErrorAction(Enum):
    """Recommended actions for error handling"""
    RETRY = "retry"
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    SKIP = "skip"
    DELETE_NODE = "delete_node"
    MARK_ERROR = "mark_error"
    ESCALATE = "escalate"
    IGNORE = "ignore"
    FALLBACK = "fallback"
    RECONFIGURE = "reconfigure"


@dataclass
class StructuredError:
    """Structured error information with comprehensive metadata"""
    error_code: str
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    details: Optional[str] = None
    provider: Optional[str] = None
    node_id: Optional[str] = None
    linkedin_username: Optional[str] = None
    recommended_action: Optional[ErrorAction] = None
    is_retryable: bool = False
    should_fallback: bool = False
    metadata: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime.datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.datetime.now(datetime.timezone.utc)
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging and storage"""
        result = asdict(self)
        # Convert enums to their values
        result['category'] = self.category.value
        result['severity'] = self.severity.value
        if self.recommended_action:
            result['recommended_action'] = self.recommended_action.value
        # Convert timestamp to ISO string
        if self.timestamp:
            result['timestamp'] = self.timestamp.isoformat()
        return result
    
    def to_log_message(self) -> str:
        """Generate a structured log message"""
        base_msg = f"[{self.error_code}] {self.message}"
        if self.provider:
            base_msg += f" (Provider: {self.provider})"
        if self.linkedin_username:
            base_msg += f" (User: {self.linkedin_username})"
        if self.details:
            base_msg += f" - {self.details}"
        return base_msg


class ErrorTaxonomy:
    """Centralized error taxonomy and classification system"""
    
    # Error code definitions with metadata
    ERROR_DEFINITIONS = {
        # API Errors
        "API_001": {
            "category": ErrorCategory.API_ERROR,
            "severity": ErrorSeverity.MEDIUM,
            "message": "API request failed",
            "is_retryable": True,
            "should_fallback": True,
            "recommended_action": ErrorAction.RETRY_WITH_BACKOFF
        },
        "API_002": {
            "category": ErrorCategory.AUTHENTICATION_ERROR,
            "severity": ErrorSeverity.HIGH,
            "message": "API authentication failed",
            "is_retryable": False,
            "should_fallback": True,
            "recommended_action": ErrorAction.FALLBACK
        },
        "API_003": {
            "category": ErrorCategory.RATE_LIMIT_ERROR,
            "severity": ErrorSeverity.MEDIUM,
            "message": "API rate limit exceeded",
            "is_retryable": True,
            "should_fallback": True,
            "recommended_action": ErrorAction.RETRY_WITH_BACKOFF
        },
        "API_004": {
            "category": ErrorCategory.API_ERROR,
            "severity": ErrorSeverity.LOW,
            "message": "Profile not found or inaccessible",
            "is_retryable": False,
            "should_fallback": False,
            "recommended_action": ErrorAction.DELETE_NODE
        },
        "API_005": {
            "category": ErrorCategory.TIMEOUT_ERROR,
            "severity": ErrorSeverity.MEDIUM,
            "message": "API request timeout",
            "is_retryable": True,
            "should_fallback": True,
            "recommended_action": ErrorAction.RETRY
        },
        
        # Data Quality Errors
        "DQ_001": {
            "category": ErrorCategory.DATA_QUALITY,
            "severity": ErrorSeverity.MEDIUM,
            "message": "Data quality below threshold",
            "is_retryable": True,
            "should_fallback": True,
            "recommended_action": ErrorAction.FALLBACK
        },
        "DQ_002": {
            "category": ErrorCategory.VALIDATION_ERROR,
            "severity": ErrorSeverity.HIGH,
            "message": "Data validation failed",
            "is_retryable": False,
            "should_fallback": True,
            "recommended_action": ErrorAction.FALLBACK
        },
        "DQ_003": {
            "category": ErrorCategory.DATA_QUALITY,
            "severity": ErrorSeverity.LOW,
            "message": "Insufficient data fields populated",
            "is_retryable": True,
            "should_fallback": True,
            "recommended_action": ErrorAction.RETRY
        },
        
        # Database Errors
        "DB_001": {
            "category": ErrorCategory.DATABASE_ERROR,
            "severity": ErrorSeverity.HIGH,
            "message": "Database connection failed",
            "is_retryable": True,
            "should_fallback": False,
            "recommended_action": ErrorAction.RETRY_WITH_BACKOFF
        },
        "DB_002": {
            "category": ErrorCategory.DATABASE_ERROR,
            "severity": ErrorSeverity.MEDIUM,
            "message": "Database operation failed",
            "is_retryable": True,
            "should_fallback": False,
            "recommended_action": ErrorAction.RETRY
        },
        "DB_003": {
            "category": ErrorCategory.DATABASE_ERROR,
            "severity": ErrorSeverity.HIGH,
            "message": "Node not found in database",
            "is_retryable": False,
            "should_fallback": False,
            "recommended_action": ErrorAction.SKIP
        },
        
        # Transformation Errors
        "TRANS_001": {
            "category": ErrorCategory.TRANSFORMATION_ERROR,
            "severity": ErrorSeverity.MEDIUM,
            "message": "Data transformation failed",
            "is_retryable": True,
            "should_fallback": True,
            "recommended_action": ErrorAction.FALLBACK
        },
        "TRANS_002": {
            "category": ErrorCategory.TRANSFORMATION_ERROR,
            "severity": ErrorSeverity.HIGH,
            "message": "Unknown provider for transformation",
            "is_retryable": False,
            "should_fallback": False,
            "recommended_action": ErrorAction.ESCALATE
        },
        
        # Configuration Errors
        "CONFIG_001": {
            "category": ErrorCategory.CONFIGURATION_ERROR,
            "severity": ErrorSeverity.CRITICAL,
            "message": "No API providers configured",
            "is_retryable": False,
            "should_fallback": False,
            "recommended_action": ErrorAction.ESCALATE
        },
        "CONFIG_002": {
            "category": ErrorCategory.CONFIGURATION_ERROR,
            "severity": ErrorSeverity.HIGH,
            "message": "Invalid configuration detected",
            "is_retryable": False,
            "should_fallback": False,
            "recommended_action": ErrorAction.RECONFIGURE
        },
        
        # Business Logic Errors
        "BL_001": {
            "category": ErrorCategory.BUSINESS_LOGIC_ERROR,
            "severity": ErrorSeverity.MEDIUM,
            "message": "Missing LinkedIn username",
            "is_retryable": False,
            "should_fallback": False,
            "recommended_action": ErrorAction.MARK_ERROR
        },
        "BL_002": {
            "category": ErrorCategory.BUSINESS_LOGIC_ERROR,
            "severity": ErrorSeverity.LOW,
            "message": "Profile already processed",
            "is_retryable": False,
            "should_fallback": False,
            "recommended_action": ErrorAction.SKIP
        },
        
        # Network Errors
        "NET_001": {
            "category": ErrorCategory.NETWORK_ERROR,
            "severity": ErrorSeverity.MEDIUM,
            "message": "Network connection failed",
            "is_retryable": True,
            "should_fallback": True,
            "recommended_action": ErrorAction.RETRY_WITH_BACKOFF
        },
        
        # Unknown Errors
        "UNK_001": {
            "category": ErrorCategory.UNKNOWN_ERROR,
            "severity": ErrorSeverity.HIGH,
            "message": "Unknown error occurred",
            "is_retryable": True,
            "should_fallback": False,
            "recommended_action": ErrorAction.ESCALATE
        }
    }
    
    @classmethod
    def create_error(cls, error_code: str, details: Optional[str] = None, 
                    provider: Optional[str] = None, node_id: Optional[str] = None,
                    linkedin_username: Optional[str] = None, 
                    metadata: Optional[Dict[str, Any]] = None) -> StructuredError:
        """Create a structured error from error code"""
        
        if error_code not in cls.ERROR_DEFINITIONS:
            # Default to unknown error
            error_code = "UNK_001"
        
        definition = cls.ERROR_DEFINITIONS[error_code]
        
        return StructuredError(
            error_code=error_code,
            category=definition["category"],
            severity=definition["severity"],
            message=definition["message"],
            details=details,
            provider=provider,
            node_id=node_id,
            linkedin_username=linkedin_username,
            recommended_action=definition.get("recommended_action"),
            is_retryable=definition.get("is_retryable", False),
            should_fallback=definition.get("should_fallback", False),
            metadata=metadata or {}
        )
    
    @classmethod
    def classify_exception(cls, exception: Exception, context: Optional[Dict[str, Any]] = None) -> StructuredError:
        """Classify an exception into a structured error"""
        context = context or {}
        
        # Classification logic based on exception type and message
        exception_str = str(exception).lower()
        exception_type = type(exception).__name__
        
        # Authentication errors
        if "unauthorized" in exception_str or "authentication" in exception_str:
            return cls.create_error("API_002", str(exception), **context)
        
        # Rate limiting
        if "rate limit" in exception_str or "429" in exception_str:
            return cls.create_error("API_003", str(exception), **context)
        
        # Timeout errors
        if "timeout" in exception_str or exception_type in ["TimeoutError", "ReadTimeoutError"]:
            return cls.create_error("API_005", str(exception), **context)
        
        # Network errors
        if "connection" in exception_str or exception_type in ["ConnectionError", "HTTPError"]:
            return cls.create_error("NET_001", str(exception), **context)
        
        # Database errors
        if "mongo" in exception_str or "database" in exception_str:
            return cls.create_error("DB_002", str(exception), **context)
        
        # Default to unknown error
        return cls.create_error("UNK_001", f"{exception_type}: {str(exception)}", **context)
    
    @classmethod
    def get_error_statistics(cls, errors: List[StructuredError]) -> Dict[str, Any]:
        """Generate statistics from a list of errors"""
        if not errors:
            return {"total": 0}
        
        stats = {
            "total": len(errors),
            "by_category": {},
            "by_severity": {},
            "by_provider": {},
            "retryable": 0,
            "fallback_recommended": 0
        }
        
        for error in errors:
            # Count by category
            category = error.category.value
            stats["by_category"][category] = stats["by_category"].get(category, 0) + 1
            
            # Count by severity
            severity = error.severity.value
            stats["by_severity"][severity] = stats["by_severity"].get(severity, 0) + 1
            
            # Count by provider
            if error.provider:
                stats["by_provider"][error.provider] = stats["by_provider"].get(error.provider, 0) + 1
            
            # Count characteristics
            if error.is_retryable:
                stats["retryable"] += 1
            if error.should_fallback:
                stats["fallback_recommended"] += 1
        
        return stats


class ErrorHandler:
    """Centralized error handling and logging system"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.error_history: List[StructuredError] = []
    
    def handle_error(self, error: StructuredError, 
                    log_level: Optional[str] = None) -> StructuredError:
        """Handle a structured error with appropriate logging and tracking"""
        
        # Determine log level based on severity if not specified
        if log_level is None:
            severity_to_log_level = {
                ErrorSeverity.LOW: "info",
                ErrorSeverity.MEDIUM: "warning", 
                ErrorSeverity.HIGH: "error",
                ErrorSeverity.CRITICAL: "critical"
            }
            log_level = severity_to_log_level.get(error.severity, "error")
        
        # Log the error
        log_message = error.to_log_message()
        getattr(self.logger, log_level)(log_message)
        
        # Log structured error details at debug level
        self.logger.debug(f"Structured error details: {error.to_dict()}")
        
        # Track error in history
        self.error_history.append(error)
        
        # Keep only recent errors (last 100)
        if len(self.error_history) > 100:
            self.error_history = self.error_history[-100:]
        
        return error
    
    def handle_exception(self, exception: Exception, 
                        context: Optional[Dict[str, Any]] = None) -> StructuredError:
        """Handle an exception by classifying it and logging"""
        error = ErrorTaxonomy.classify_exception(exception, context)
        return self.handle_error(error)
    
    def get_recent_errors(self, limit: int = 50) -> List[StructuredError]:
        """Get recent errors for debugging"""
        return self.error_history[-limit:]
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of recent errors"""
        recent_errors = self.get_recent_errors()
        return ErrorTaxonomy.get_error_statistics(recent_errors)
    
    def clear_error_history(self):
        """Clear error history"""
        self.error_history.clear()


# Global error handler instance
error_handler = ErrorHandler()


# Convenience functions for common error scenarios
def create_api_error(details: str, provider: str = None, node_id: str = None, 
                    linkedin_username: str = None) -> StructuredError:
    """Create an API error"""
    return ErrorTaxonomy.create_error("API_001", details, provider, node_id, linkedin_username)


def create_data_quality_error(details: str, provider: str = None, node_id: str = None,
                             linkedin_username: str = None, quality_score: int = None) -> StructuredError:
    """Create a data quality error"""
    metadata = {"quality_score": quality_score} if quality_score is not None else None
    return ErrorTaxonomy.create_error("DQ_001", details, provider, node_id, linkedin_username, metadata)


def create_database_error(details: str, node_id: str = None) -> StructuredError:
    """Create a database error"""
    return ErrorTaxonomy.create_error("DB_002", details, None, node_id)


def create_configuration_error(details: str) -> StructuredError:
    """Create a configuration error"""
    return ErrorTaxonomy.create_error("CONFIG_002", details)