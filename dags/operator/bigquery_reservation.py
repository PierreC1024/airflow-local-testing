"""This module contains a BigQuery Reservation Hook."""
from __future__ import annotations

from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseHook,
)
from airflow.providers.google.common.consts import CLIENT_INFO

from google.cloud.bigquery_reservation_v1 import (
    ReservationServiceClient,
    CapacityCommitment,
    Reservation,
    Assignment,
)
from google.api_core import retry
from airflow.exceptions import AirflowException


class BiqQueryReservationServiceHook(GoogleBaseHook):
    """
    Hook for Google Bigquery Reservatuin API.
    """

    def __init__(
        self,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        location: str | None = None,
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.location = location
        self.running_job_id: str | None = None

    def get_conn(self) -> ReservationServiceClient:
        """
        Retrieves connection to Google Bigquery.

        :return: Google Bigquery Reservation API client
        """
        return ReservationServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_flex_slots_reservation(
        self, slots, project_id: str = PROVIDE_PROJECT_ID
    ) -> None:
        """
        Create a commitment for a specific amount of slots.

        :param slots: Number of slots to purchase
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{self.location}"

        if slots % 100:
            raise AirflowException(
                "Commitment slots can only be reserved in increments of 100."
            )

        try:
            # 1. Create Capacity Commitment
            # 2. Create Reservation
            # 3. Create Assigment
            commitment = client.create_capacity_commitment(
                parent=parent,
                capacity_commitment=CapacityCommitment(plan="FLEX", slot_count=slots),
            )

            self.commitment = commitment.name

            reservation = client.create_reservation(
                parent=parent,
                reservation_id=f"c-{commitment.name.split('/')[-1]}",
                reservation=Reservation(slot_capacity=slots, ignore_idle_slots=True),
            )

            self.reservation = reservation.name

            assignment = client.create_assignment(
                parent=reservation.name,
                assignment=Assignment(
                    job_type="QUERY", assignee=f"projects/{project_id}"
                ),
            )
            self.assignment = assignment.name

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to purchase {slots} flex BigQuery slots commitments (parent: {parent}, commitment: {commitment.name})."
            )

    def delete_flex_slots_reservation(self) -> None:
        """
        Delete a flex slots reserved
        """
        client = self.get_conn()

        try:
            client.delete_assignment(self.assignment)
            client.delete_reservation(self.reservation)
            client.delete_capacity_commitment(
                name=self.commitment,
                retry=retry.Retry(deadline=90, predicate=Exception, maximum=2),
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to delete flex BigQuery slots("
                + "assignement: {self.assignement}, "
                + "reservation: {self.reservation}, "
                + "commitments: {self.commitment}."
            )
