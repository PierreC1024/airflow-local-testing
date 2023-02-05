"""This module contains Google BigQuery Run operators."""
from __future__ import annotations

import enum
import json
import warnings
from typing import TYPE_CHECKING, Any, Sequence

import attr
from google.api_core.exceptions import Conflict
from google.api_core.retry import Retry
from google.cloud.bigquery import DEFAULT_RETRY, CopyJob, ExtractJob, LoadJob, QueryJob

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.xcom import XCom
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.links.bigquery import BigQueryTableLink
from airflow.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger

from dags.operator.bigquery_reservation import BiqQueryReservationServiceHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


BIGQUERY_JOB_DETAILS_LINK_FMT = "https://console.cloud.google.com/bigquery?j={job_id}"


class BigQueryUIColors(enum.Enum):
    """Hex colors for BigQuery operators"""

    CHECK = "#C0D7FF"
    QUERY = "#A1BBFF"
    TABLE = "#81A0FF"
    DATASET = "#5F86FF"


class BigQueryInsertJobOperatorV2(BaseOperator):
    """
    Executes a BigQuery job. Waits for the job to complete and returns job id.
    This operator work in the following way:

    - it calculates a unique hash of the job using job's configuration or uuid if ``force_rerun`` is True
    - creates ``job_id`` in form of
        ``[provided_job_id | airflow_{dag_id}_{task_id}_{exec_date}]_{uniqueness_suffix}``
    - submits a BigQuery job using the ``job_id``
    - if job with given id already exists then it tries to reattach to the job if its not done and its
        state is in ``reattach_states``. If the job is done the operator will raise ``AirflowException``.

    Using ``force_rerun`` will submit a new job every time without attaching to already existing ones.

    For job definition see here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryInsertJobOperator`


    :param configuration: The configuration parameter maps directly to BigQuery's
        configuration field in the job object. For more details see
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    :param job_id: The ID of the job. It will be suffixed with hash of job configuration
        unless ``force_rerun`` is True.
        The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
        dashes (-). The maximum length is 1,024 characters. If not provided then uuid will
        be generated.
    :param force_rerun: If True then operator will use hash of uuid as job id suffix
    :param reattach_states: Set of BigQuery job's states in case of which we should reattach
        to the job. Should be other than final states.
    :param project_id: Google Cloud Project where the job is running
    :param location: location the job is running
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    :param result_retry: How to retry the `result` call that retrieves rows
    :param result_timeout: The number of seconds to wait for `result` method before using `result_retry`
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields: Sequence[str] = (
        "configuration",
        "job_id",
        "impersonation_chain",
        "project_id",
    )
    template_ext: Sequence[str] = (
        ".json",
        ".sql",
    )
    template_fields_renderers = {
        "configuration": "json",
        "configuration.query.query": "sql",
    }
    ui_color = BigQueryUIColors.QUERY.value
    operator_extra_links = (BigQueryTableLink(),)

    # ToDo: Add parameter to choose if you use flex provisioner

    def __init__(
        self,
        configuration: dict[str, Any],
        project_id: str | None = None,
        location: str | None = None,
        job_id: str | None = None,
        force_rerun: bool = True,
        reattach_states: set[str] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        result_retry: Retry = DEFAULT_RETRY,
        result_timeout: float | None = None,
        deferrable: bool = False,
        flex_slot_provisioning: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.configuration = configuration
        self.location = location
        self.job_id = job_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'",
                DeprecationWarning,
            )
        self.delegate_to = delegate_to
        self.force_rerun = force_rerun
        self.reattach_states: set[str] = reattach_states or set()
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.result_retry = result_retry
        self.result_timeout = result_timeout
        self.hook: BigQueryHook | None = None
        self.deferrable = deferrable
        self.flex_slot_provisioning = flex_slot_provisioning

    def prepare_template(self) -> None:
        # If .json is passed then we have to read the file
        if isinstance(self.configuration, str) and self.configuration.endswith(".json"):
            with open(self.configuration) as file:
                self.configuration = json.loads(file.read())

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        # Submit a new job without waiting for it to complete.
        return hook.insert_job(
            configuration=self.configuration,
            project_id=self.project_id,
            location=self.location,
            job_id=job_id,
            timeout=self.result_timeout,
            retry=self.result_retry,
            nowait=True,
        )

    @staticmethod
    def _handle_job_error(job: BigQueryJob) -> None:
        if job.error_result:
            raise AirflowException(
                f"BigQuery job {job.job_id} failed: {job.error_result}"
            )

    def execute(self, context: Any):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook

        job_id = hook.generate_job_id(
            job_id=self.job_id,
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=context["logical_date"],
            configuration=self.configuration,
            force_rerun=self.force_rerun,
        )

        # Slots provisioning
        if self.flex_slot_provisioning:
            hook_reservation = BiqQueryReservationServiceHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
                location=self.location,
            )

            self.hook_reservation = hook_reservation

            hook_reservation.create_flex_slots_reservation(
                slots=self.flex_slot_provisioning
            )

        try:
            self.log.info("Executing: %s'", self.configuration)
            job = self._submit_job(hook, job_id)
        except Conflict:
            # If the job already exists retrieve it
            job = hook.get_job(
                project_id=self.project_id,
                location=self.location,
                job_id=job_id,
            )
            if job.state in self.reattach_states:
                # We are reattaching to a job
                job._begin()
                self._handle_job_error(job)
            else:
                # Same job configuration so we need force_rerun
                raise AirflowException(
                    f"Job with id: {job_id} already exists and is in {job.state} state. If you "
                    f"want to force rerun it consider setting `force_rerun=True`."
                    f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
                )

        job_types = {
            LoadJob._JOB_TYPE: ["sourceTable", "destinationTable"],
            CopyJob._JOB_TYPE: ["sourceTable", "destinationTable"],
            ExtractJob._JOB_TYPE: ["sourceTable"],
            QueryJob._JOB_TYPE: ["destinationTable"],
        }

        if self.project_id:
            for job_type, tables_prop in job_types.items():
                job_configuration = job.to_api_repr()["configuration"]
                if job_type in job_configuration:
                    for table_prop in tables_prop:
                        if table_prop in job_configuration[job_type]:
                            table = job_configuration[job_type][table_prop]
                            persist_kwargs = {
                                "context": context,
                                "task_instance": self,
                                "project_id": self.project_id,
                                "table_id": table,
                            }
                            if not isinstance(table, str):
                                persist_kwargs["table_id"] = table["tableId"]
                                persist_kwargs["dataset_id"] = table["datasetId"]

                            BigQueryTableLink.persist(**persist_kwargs)

        self.job_id = job.job_id
        context["ti"].xcom_push(key="job_id", value=self.job_id)
        # Wait for the job to complete
        if not self.deferrable:
            job.result(timeout=self.result_timeout, retry=self.result_retry)
            self._handle_job_error(job)

            return self.job_id
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryInsertJobTrigger(
                conn_id=self.gcp_conn_id,
                job_id=self.job_id,
                project_id=self.project_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]):
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if self.flex_slot_provisioning:
            self.hook_reservation.delete_flex_slots_reservation()

        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
        return self.job_id

    def on_kill(self) -> None:
        if self.job_id and self.cancel_on_kill:
            self.hook.cancel_job(  # type: ignore[union-attr]
                job_id=self.job_id, project_id=self.project_id, location=self.location
            )

        if self.flex_slot_provisioning:
            self.hook_reservation.delete_flex_slots_reservation()

        else:
            self.log.info(
                "Skipping to cancel job: %s:%s.%s",
                self.project_id,
                self.location,
                self.job_id,
            )
