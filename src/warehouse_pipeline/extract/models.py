from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ExtractModel(BaseModel):
    """
    Base model for raw extracted source validation.

    Ignores extra upstream fields so harmless API additions
    do not break extraction.
    """

    model_config = ConfigDict(extra="ignore")


class DummyAddress(ExtractModel):
    """Adress info."""

    city: str | None = None
    country: str | None = None


class DummyCompany(ExtractModel):
    """Company name info."""

    name: str | None = None


class DummyUser(ExtractModel):
    """Defining a User's info."""

    id: int = Field(gt=0)
    firstName: str
    lastName: str
    email: str | None = None
    phone: str | None = None
    birthDate: str | None = None
    address: DummyAddress | None = None
    company: DummyCompany | None = None

    @field_validator("firstName", "lastName")
    @classmethod
    def _strip_and_require_non_blank(cls, value: str) -> str:
        """`firstName` and `lastName` must not be extracted blank."""
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


class DummyProduct(ExtractModel):
    """Defining  a Product's info."""

    id: int = Field(gt=0)
    title: str
    category: str
    price: float = Field(ge=0)
    stock: int = Field(ge=0)

    brand: str | None = None
    discountedTotal: float | None = Field(default=None, ge=0)
    rating: float | None = Field(default=None, ge=0)

    @field_validator("title", "category")
    @classmethod
    def _strip_and_require_non_blank(cls, value: str) -> str:
        """`title` and `category` must not be extracted blank."""
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


class DummyCartProduct(ExtractModel):
    """Defining a cart's inner product."""

    id: int = Field(gt=0)
    quantity: int = Field(ge=0)
    price: float = Field(ge=0)
    total: float = Field(ge=0)
    discountedTotal: float | None = Field(default=None, ge=0)


class DummyCart(ExtractModel):
    """Defining a Cart's (an order's) info."""

    id: int = Field(gt=0)
    userId: int = Field(gt=0)
    total: float = Field(ge=0)
    discountedTotal: float = Field(ge=0)
    totalProducts: int = Field(ge=0)
    totalQuantity: int = Field(ge=0)
    products: list[DummyCartProduct]


class PageEnvelope(ExtractModel):
    """Contains page data within extraction."""

    total: int = Field(ge=0)
    skip: int = Field(ge=0)
    limit: int = Field(gt=0)


class UsersPage(PageEnvelope):
    """Returned data for the Users page."""

    users: list[DummyUser]


class ProductsPage(PageEnvelope):
    """Data for Product's page."""

    products: list[DummyProduct]


class CartsPage(PageEnvelope):
    """Data for Cart's (an order's) page."""

    carts: list[DummyCart]


def parse_users_page(payload: Mapping[str, Any]) -> UsersPage:
    """Parse the users page."""
    return UsersPage.model_validate(payload)


def parse_products_page(payload: Mapping[str, Any]) -> ProductsPage:
    """Parse the products page."""
    return ProductsPage.model_validate(payload)


def parse_carts_page(payload: Mapping[str, Any]) -> CartsPage:
    """Parse the carts page."""
    return CartsPage.model_validate(payload)
