"""HTTP routing, request validation and translation of application results."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request

from .models import Incident, IncidentList, IncidentStatus, Overview
from .queries import IncidentQueries

router = APIRouter(prefix="/api/v1", tags=["incidents"])


def get_queries(request: Request) -> IncidentQueries:
    return IncidentQueries(request.app.state.repository)


Queries = Annotated[IncidentQueries, Depends(get_queries)]


@router.get("/overview", response_model=Overview)
def overview(queries: Queries) -> Overview:
    return queries.overview()


@router.get("/incidents", response_model=IncidentList)
def incidents(queries: Queries, status: IncidentStatus | None = None) -> IncidentList:
    return queries.list_incidents(status)


@router.get("/incidents/{incident_id}", response_model=Incident)
def incident(incident_id: str, queries: Queries) -> Incident:
    item = queries.get_incident(incident_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return item
