from functools import partial
from typing import Any, Optional, List, Dict
from uuid import UUID
from datetime import datetime, date
from decimal import Decimal
from enum import Enum

from asyncpg import Record
from orjson import dumps, OPT_NON_STR_KEYS, OPT_SERIALIZE_NUMPY, OPT_INDENT_2
from starlette.responses import JSONResponse
from fastapi import Request
from pydantic import BaseModel

from app.core.logger import api_logger


def format_media_url(request: Request, path: str) -> str | None:
    """
    Args:
        request: FastAPI Request object
        path: Relative media path

    Returns:
        Full URL to media file
    """
    if not path:
        return None

    normalized_path = path.replace('\\', '/')

    try:
        return str(request.url_for('media', path=normalized_path))
    except Exception as e:
        api_logger.error(f"Error formatting media URL for path '{path}': {e}")
        return path


def default(obj: Any, request: Request = None) -> Any:
    """
    Args:
        obj: Object to serialize
        request: FastAPI Request (for media URL generation)

    Returns:
        Serializable representation
    """

    if isinstance(obj, UUID):
        return str(obj)

    if isinstance(obj, Record):
        d = dict(obj)

        if request:
            if img := d.get('image'):
                d['image'] = format_media_url(request, img)

            for field in ('avatar', 'photo', 'thumbnail', 'logo', 'banner'):
                if value := d.get(field):
                    d[field] = format_media_url(request, value)

            if images := d.get('images'):
                if isinstance(images, (list, tuple)):
                    d['images'] = [format_media_url(request, img) for img in images if img]

        return d

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, date):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, Enum):
        return obj.value

    if isinstance(obj, BaseModel):
        return obj.model_dump()

    return obj


class ORJSONResponse(JSONResponse):
    """
    Custom JSON response using ORJSON for fast serialization
    """

    def __init__(
            self,
            content: Any,
            request: Request = None,
            status_code: int = 200,
            headers: Optional[Dict[str, str]] = None,
            media_type: str = "application/json",
            pretty: bool = False,
    ):
        """
        Initialize ORJSON response

        Args:
            content: Response content
            request: FastAPI Request
            status_code: HTTP status code
            headers: Additional headers
            media_type: Content type
            pretty: Pretty print JSON (adds indentation)
        """
        self._request = request
        self._pretty = pretty
        super().__init__(
            content=content,
            status_code=status_code,
            headers=headers,
            media_type=media_type
        )

    def render(self, content: Any) -> bytes:
        """
        Render content to JSON bytes using ORJSON

        Args:
            content: Content to serialize

        Returns:
            JSON bytes
        """
        options = OPT_NON_STR_KEYS | OPT_SERIALIZE_NUMPY

        if self._pretty:
            options |= OPT_INDENT_2

        try:
            return dumps(
                content,
                option=options,
                default=partial(default, request=self._request)
            )
        except Exception as e:
            api_logger.error(f"Error serializing response: {e}")

            return dumps({
                "success": False,
                "message": "Serialization error",
                "error": str(e)
            })


def standard_response(
        success: bool,
        message: str,
        data: Any = None,
        errors: Optional[List[str]] = None,
        meta: Optional[Dict[str, Any]] = None,
        request: Request = None,
        status_code: int = 200,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Create standardized API response

    Response format:
    {
        "success": true/false,
        "message": "Operation description",
        "data": {...},           // Optional
        "errors": [...],         // Optional
        "meta": {...},           // Optional (pagination, etc)
        "timestamp": "ISO8601"
    }

    Args:
        success: Operation success status
        message: Human-readable message
        data: Response data (optional)
        errors: List of error messages (optional)
        meta: Metadata (pagination, counts, etc)
        request: FastAPI Request
        status_code: HTTP status code
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse
    """
    response: dict = {
        'success': success,
        'message': message,
        'timestamp': datetime.now().isoformat() + 'Z'
    }

    if data is not None:
        response['data'] = data

    if errors is not None:
        response['errors'] = errors

    if meta is not None:
        response['meta'] = meta

    return ORJSONResponse(
        content=response,
        request=request,
        status_code=status_code,
        pretty=pretty
    )


def success_response(
        message: str = "Success",
        data: Any = None,
        meta: Optional[Dict[str, Any]] = None,
        request: Request = None,
        status_code: int = 200,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Args:
        message: Success message
        data: Response data
        meta: Metadata
        request: FastAPI Request
        status_code: HTTP status code
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse
    """
    return standard_response(
        success=True,
        message=message,
        data=data,
        meta=meta,
        request=request,
        status_code=status_code,
        pretty=pretty
    )


def error_response(
        message: str = "Error",
        errors: Optional[List[str]] = None,
        data: Any = None,
        request: Request = None,
        status_code: int = 400,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Args:
        message: Error message
        errors: List of detailed errors
        data: Additional error data
        request: FastAPI Request
        status_code: HTTP status code (default: 400)
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse
    """
    return standard_response(
        success=False,
        message=message,
        errors=errors,
        data=data,
        request=request,
        status_code=status_code,
        pretty=pretty
    )


def paginated_response(
        data: List[Any],
        total: int,
        page: int,
        page_size: int,
        message: str = "Data retrieved successfully",
        request: Request = None,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Args:
        data: List of items for current page
        total: Total number of items
        page: Current page number (1-indexed)
        page_size: Items per page
        message: Success message
        request: FastAPI Request
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse with pagination metadata
    """
    total_pages = (total + page_size - 1) // page_size

    meta = {
        'pagination': {
            'page': page,
            'page_size': page_size,
            'total_items': total,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1,
        }
    }

    return success_response(
        message=message,
        data=data,
        meta=meta,
        request=request,
        status_code=200,
        pretty=pretty
    )


def validation_error_response(
        errors: List[str],
        message: str = "Validation error",
        request: Request = None,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Args:
        errors: List of validation errors
        message: Error message
        request: FastAPI Request
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse with 422 status
    """
    return error_response(
        message=message,
        errors=errors,
        request=request,
        status_code=422,
        pretty=pretty
    )


def not_found_response(
        message: str = "Resource not found",
        resource: Optional[str] = None,
        request: Request = None,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Args:
        message: Error message
        resource: Resource identifier
        request: FastAPI Request
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse with 404 status
    """
    data = None
    if resource:
        data = {"resource": resource}

    return error_response(
        message=message,
        data=data,
        request=request,
        status_code=404,
        pretty=pretty
    )


def unauthorized_response(
        message: str = "Unauthorized",
        request: Request = None,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Args:
        message: Error message
        request: FastAPI Request
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse with 401 status
    """
    return error_response(
        message=message,
        request=request,
        status_code=401,
        pretty=pretty
    )


def forbidden_response(
        message: str = "Forbidden",
        request: Request = None,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Args:
        message: Error message
        request: FastAPI Request
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse with 403 status
    """
    return error_response(
        message=message,
        request=request,
        status_code=403,
        pretty=pretty
    )


def server_error_response(
        message: str = "Internal server error",
        error_id: Optional[str] = None,
        request: Request = None,
        pretty: bool = False,
) -> ORJSONResponse:
    """
    Args:
        message: Error message
        error_id: Error tracking ID
        request: FastAPI Request
        pretty: Pretty print JSON

    Returns:
        ORJSONResponse with 500 status
    """
    data = None
    if error_id:
        data = {"error_id": error_id}

    return error_response(
        message=message,
        data=data,
        request=request,
        status_code=500,
        pretty=pretty
    )
