from __future__ import annotations

from typing import Iterable

from warehouse_pipeline.extract.models import DummyUser
from warehouse_pipeline.stage import MappedUsers, StageReject, StageRow, UserLookupItem
from warehouse_pipeline.stage.derive_fields import derive_full_name, normalize_email, normalize_text


def map_users(users: Iterable[DummyUser]) -> MappedUsers:
    """Map validated DummyJSON users into `stg_customers` rows and a user lookup."""
    rows: list[StageRow] = []
    rejects: list[StageReject] = []
    user_lookup: dict[int, UserLookupItem] = {}

    for source_ref, user in enumerate(users, start=1):
        raw_payload = user.model_dump(mode="python")

        first_name = normalize_text(user.firstName)
        last_name = normalize_text(user.lastName)
        full_name = derive_full_name(first_name, last_name)

        if full_name is None:
            rejects.append(
                StageReject(
                    table_name="stg_customers",
                    source_ref=source_ref,
                    raw_payload=raw_payload,
                    reason_code="missing_name",
                    reason_detail="user could not be mapped because both first_name and last_name are blank",
                )
            )
            continue


        email = normalize_email(user.email)
        city = normalize_text(user.address.city) if user.address else None
        country = normalize_text(user.address.country) if user.address else None
        company = normalize_text(user.company.name) if user.company else None

        rows.append(
            StageRow(
                table_name="stg_customers",
                source_ref=source_ref,
                raw_payload=raw_payload,
                values={
                    "customer_id": user.id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": full_name,
                    "email": email,
                    "phone": normalize_text(user.phone),
                    "city": city,
                    "country": country,
                    "company": company,
                },
            )
        )            

        # Keep the first-seen lookup value.
        # (lowest `source_ref` wins).
        user_lookup.setdefault(
            user.id,
            UserLookupItem(
                customer_id=user.id,
                country=country,
                city=city,
                email=email,
            ),
        )

    return MappedUsers(rows=rows, rejects=rejects, user_lookup=user_lookup)