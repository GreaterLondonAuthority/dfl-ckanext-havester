from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, TypeVar, Generic, Any, Iterable
from datetime import datetime
from abc import ABC, abstractmethod
from hashlib import sha1


@dataclass
class SimpleStandard:
    # todo decide relationship to dublincore
    # todo possibly make use of ckan default schema?

    # package_id becomes id in an actual package
    package_id: str

    # fields required for search
    title: str
    description: str

    # fields to include by default (eg to avoid being marked as changed)
    url: str = ""
    version: str = ""
    author: str = ""
    author_email: str = ""
    license_id: str = ""
    license_title: str = ""

    # other fields
    org_name: Optional[str] = None
    org_link: Optional[str] = None
    private: Optional[bool] = None
    maintainer: Optional[str] = None
    maintainer_email: Optional[str] = None
    geography_level: Optional[str] = None
    update_frequency: Optional[str] = None
    state: Optional[str] = None
    resources: Optional[list[dict[Any, Any]]] = None
    data_updated_at: Optional[datetime] = None
    metadata_modified: Optional[datetime] = None

    # extras fields todo unclear where/how these are used by the library
    upstream_url: Optional[str] = None
    upstream_metadata_created: Optional[datetime] = None
    upstream_metadata_modified: Optional[datetime] = None
    upstream_publication_date: Optional[datetime] = None

    unmanaged_fields: Optional[dict[Any, Any]] = None

    @staticmethod
    def create_hashed_id(text: str) -> str:
        # todo is this the best place for this
        s = sha1()
        s.update(text.encode())
        return s.hexdigest()

    def as_dfl_package(self) -> dict[str, Any]:
        # todo do this more intelligently
        # todo decide on common date behaviour
        package = {
            "id": self.package_id,
            "name": self.title,
            "title": self.title,
            "description": self.description,
            "notes": self.description,
            "owner_org": self.org_name,
            "private": self.private,
            "author": self.author or "",
            "author_email": self.author_email or "",
            "maintainer": self.maintainer,
            "maintainer_email": self.maintainer_email,
            "org_name": self.org_name,
            "org_link": self.org_link,
            "license_id": self.license_id or "",
            "license_title": self.license_title or "",
            "url": self.url or "",
            "version": self.version or "",
            "state": self.state,
            "resources": self.resources,
            "data_updated_at": self.data_updated_at,
            "metadata_modified": self.metadata_modified,
        }

        # prioritise specified fields ahead of unmanaged, restructure to ckan list format
        extras = {
            "upstream_url": self.upstream_url,
            "upstream_metadata_created": self.upstream_metadata_created,
            "upstream_metadata_modified": self.metadata_modified,
            "upstream_publication_date": self.upstream_publication_date
        }

        if self.unmanaged_fields:
            extras = {**self.unmanaged_fields, **extras}

        package["extras"] = [{"key": k, "value": v} for k, v in extras.items() if v is not None]

        # don't keep Nones but do keep falsy values
        package = {k: v for k, v in package.items() if v is not None}

        return package


# Define child classes w/ e.g. MyClass(Collector[dict]) to enforce T as a dict
T = TypeVar("T")


class Collector(ABC, Generic[T]):

    @abstractmethod
    def gather(self) -> Iterable[str]:
        # gather urls which need to be harvested from
        pass

    @abstractmethod
    def fetch(self, url: str) -> T:
        # fetch metadata from required url
        pass

    @abstractmethod
    def transform(self, received: T, upstream_url: str, org_name: str) -> Iterable[SimpleStandard]:
        # make any adjustments before import
        pass
