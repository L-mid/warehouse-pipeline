from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Union, Literal



# -- Protocols: match existing types for parsers

 
class ParsedRowProto(Protocol):
    """
    Assign a sucessfully parsed row for further use in the data ingestion pipeline.
    """
    # is representable as mapping to allow staging insertation
    def to_mapping(self) -> Mapping[str, Any]: ...


class RejectRowProto(Protocol):
    """
    Assign a rejected row for insertation into the DB's table `reject_rows`.
    """
    table_name: str
    source_row: int
    raw_payload: Mapping[str, Any]
    reason_code: Any  # can be enum or str
    reason_detail: str

ParseResult = Union[ParsedRowProto, RejectRowProto]     # simply a parsed result type.



class RowParserProto(Protocol):
    """Protocal to initalize parsing a row. Returns `ParseResult`."""
    def parse(self, raw: Mapping[str, Any], *, source_row: int) -> ParseResult: ...


# input expectations for each table spec
InputFormat = Literal["csv", "jsonl"]



@dataclass(frozen=True)
class TableSpec:
    """Contains a DB's table expectations."""
    table_name: str
    input_format: InputFormat
    parser: RowParserProto      # which parser this table expects



def get_table_spec(table_name: str) -> TableSpec:
    """
    A registry that assigns a DB table its expected parser. `FieldSpec` defines parsing rules inside the parser modules.
    """
    if table_name == "stg_customers":
        from .profiles.customers import CUSTOMER_PARSER  
        return TableSpec(table_name="stg_customers", input_format="csv", parser=CUSTOMER_PARSER)

    if table_name == "stg_retail_transactions":
        from .profiles.retail_transactions import RETAIL_TRANSACTIONS_PARSER  
        return TableSpec(table_name="stg_retail_trasactions", input_format="csv", parser=RETAIL_TRANSACTIONS_PARSER)

    raise ValueError(f"Unknown table_name: {table_name}")


