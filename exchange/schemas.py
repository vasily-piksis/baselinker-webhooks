"""Pydantic schemas for Exchange API requests and responses."""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class BaseBLRequest(BaseModel):
    """Base request schema for BaseLinker-style payloads.

    Attributes:
        model_config: Pydantic config allowing extra fields.
    """

    model_config = ConfigDict(extra="allow")


PageNumber = Annotated[int, Field(ge=1, description="Page number (>=1)")]
ProductsPerPage = Annotated[int, Field(ge=1, le=1000, description="Items per page (1-1000)")]
OrdersPerPage = Annotated[int, Field(ge=1, le=100, description="Items per page (1-100)")]


class ProductsListRequest(BaseBLRequest):
    """Request schema for listing products.

    Attributes:
        page: Page number (>=1).
        per_page: Items per page (1-1000).
        status: Optional Discogs status filter.
    """

    page: PageNumber = 1
    per_page: ProductsPerPage = 1000
    status: Optional[str] = Field(None, description="Optional Discogs status filter")


class ProductDataRequest(BaseBLRequest):
    """Request schema for fetching product data.

    Attributes:
        products_id: Official Shop API comma-separated product identifiers.
        products: Product identifiers or list of identifiers.
        ids: Alternate identifier list.
        listing_ids: Discogs listing identifiers.
    """

    products_id: Optional[List[str] | str] = None
    products: Optional[List[str] | str] = None
    ids: Optional[List[str] | str] = None
    listing_ids: Optional[List[str] | str] = None


class OrdersListRequest(BaseBLRequest):
    """Request schema for listing orders.

    Attributes:
        page: Page number (>=1).
        per_page: Items per page (1-100).
        status: Optional Discogs status filter.
        date_from: BaseLinker time_from timestamp used as the local last-activity cutoff.
    """

    page: PageNumber = 1
    per_page: OrdersPerPage = 50
    status: Optional[str] = Field(None, description="Discogs status filter")
    date_from: Optional[int] = Field(
        None,
        description="BaseLinker time_from timestamp used as the local last-activity cutoff",
    )


class OrdersGetRequest(BaseBLRequest):
    """Request schema for fetching a single order.

    Attributes:
        order_id: Discogs order id.
    """

    order_id: str = Field(..., min_length=1, description="Discogs order id")


class OrdersStatusRequest(BaseBLRequest):
    """Request schema for updating an order status.

    Attributes:
        order_id: Discogs order id.
        status: Discogs status value.
        tracking_number: Tracking number.
        shipping_provider: Carrier name.
        shipping_method: Shipping method name.
        message: Optional buyer-facing message.
        message_status: Status to send with the message.
    """

    order_id: str = Field(..., min_length=1, description="Discogs order id")
    status: Optional[str] = Field(None, description="Discogs status value")
    tracking_number: Optional[str] = Field(None, description="Tracking number")
    shipping_provider: Optional[str] = Field(None, description="Carrier name")
    shipping_method: Optional[str] = None
    message: Optional[str] = Field(None, description="Optional buyer-facing message")
    message_status: Optional[str] = Field(None, description="Status to send with the message")


class ProductsListResponse(BaseModel):
    """Response schema for listing products.

    Attributes:
        model_config: Pydantic config allowing extra fields.
        status: Response status ("OK").
        counter: Total product count.
        page: Current page number.
        per_page: Items per page.
        last_page: Whether this is the last page.
        products: List of product payloads.
        updated_at: Optional last update timestamp.
    """

    model_config = ConfigDict(extra="allow")
    status: Literal["OK"]
    counter: int
    page: int
    per_page: int
    last_page: bool
    products: List[Dict[str, Any]]
    updated_at: Optional[str] = None


class ProductDataResponse(BaseModel):
    """Response schema for product data.

    Attributes:
        model_config: Pydantic config allowing extra fields.
        status: Response status ("OK").
        products: List of product payloads.
        count: Total products returned.
    """

    model_config = ConfigDict(extra="allow")
    status: Literal["OK"]
    products: List[Dict[str, Any]]
    count: int


class OrdersListResponse(BaseModel):
    """Response schema for listing orders.

    Attributes:
        model_config: Pydantic config allowing extra fields.
        status: Response status ("OK").
        page: Current page number.
        per_page: Items per page.
        counter: Total order count.
        orders: List of order payloads.
    """

    model_config = ConfigDict(extra="allow")
    status: Literal["OK"]
    page: int
    per_page: int
    counter: int
    orders: List[Dict[str, Any]]


class OrdersGetResponse(BaseModel):
    """Response schema for fetching a single order.

    Attributes:
        model_config: Pydantic config allowing extra fields.
        status: Response status ("OK").
        order: Order payload dictionary.
    """

    model_config = ConfigDict(extra="allow")
    status: Literal["OK"]
    order: Dict[str, Any]


class OrdersStatusResponse(BaseModel):
    """Response schema for order status updates.

    Attributes:
        model_config: Pydantic config allowing extra fields.
        status: Response status ("OK").
        order_id: Discogs order id.
        updated_fields: Fields updated by the request.
        message_sent: Whether a message was posted.
    """

    model_config = ConfigDict(extra="allow")
    status: Literal["OK"]
    order_id: str
    updated_fields: List[str] | str
    message_sent: bool


class ErrorResponse(BaseModel):
    """Error response schema.

    Attributes:
        model_config: Pydantic config allowing extra fields.
        status: Response status ("ERROR").
        code: Error code.
        message: Error message.
        correlation_id: Correlation id for tracing.
        request_id: Request identifier if available.
        details: Optional error details payload.
    """

    model_config = ConfigDict(extra="allow")
    status: Literal["ERROR"]
    code: str
    message: str
    correlation_id: Optional[str] = None
    request_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
