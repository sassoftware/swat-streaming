# Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import uuid
from typing import List, Dict, Any, Optional

import pandas as pd
import swat
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import CASConfig

log = logging.getLogger(__name__)


class CASWriter:
    def __init__(self, cfg: CASConfig):
        self.cfg = cfg
        self._conn: Optional[swat.CAS] = None
        self._table_initialized = False
        self._instance_id = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"
        self._batch_seq = 0

    def connect(self) -> None:
        kwargs: Dict[str, Any] = {
            "hostname": self.cfg.hostname,
            "protocol": self.cfg.protocol,
        }
        if self.cfg.port:
            kwargs["port"] = self.cfg.port
        if self.cfg.ssl_ca_list:
            kwargs["ssl_ca_list"] = self.cfg.ssl_ca_list

        if self.cfg.token:
            kwargs["password"] = self.cfg.token
        else:
            kwargs["username"] = self.cfg.username
            kwargs["password"] = self.cfg.password

        log.info(
            "Connecting to CAS",
            extra={
                "host": self.cfg.hostname,
                "protocol": self.cfg.protocol,
                "auth": "token" if self.cfg.token else "password",
            },
        )
        self._conn = swat.CAS(**kwargs)
        self._table_initialized = self._target_table_exists()
        log.info(
            "CAS session established",
            extra={"existing_target_table": self._table_initialized},
        )

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            if self.cfg.persist_on_shutdown and self._table_initialized:
                self._save_table()
        finally:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                log.warning("Error while closing CAS connection", exc_info=True)
            self._conn = None

    def _target_table_exists(self) -> bool:
        result = self._conn.table.tableExists(
            name=self.cfg.table_name, caslib=self.cfg.caslib
        )
        return bool(result.get("exists", 0))

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(swat.SWATError),
    )
    def write_batch(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        if self._conn is None:
            raise RuntimeError("CASWriter is not connected")

        frame = pd.DataFrame.from_records(records)

        if not self._table_initialized:
            self._initial_load(frame)
        else:
            self._append(frame)

    def _initial_load(self, frame: pd.DataFrame) -> None:
        casout: Dict[str, Any] = {
            "name": self.cfg.table_name,
            "caslib": self.cfg.caslib,
            "replace": True,
        }
        if self.cfg.promote:
            casout["promote"] = True
        result = self._conn.upload_frame(frame, casout=casout)
        if result.severity > 1:
            raise swat.SWATError(f"CAS initial load failed: {result.messages}")
        self._table_initialized = True
        log.info(
            "Initial CAS table created",
            extra={
                "rows": len(frame),
                "table": self.cfg.table_name,
                "caslib": self.cfg.caslib,
                "promoted": self.cfg.promote,
            },
        )

    def _append(self, frame: pd.DataFrame) -> None:
        self._batch_seq += 1
        tmp_name = f"{self.cfg.table_name}_b_{self._instance_id}_{self._batch_seq}"
        upload = self._conn.upload_frame(
            frame,
            casout={"name": tmp_name, "caslib": self.cfg.caslib, "replace": True},
        )
        if upload.severity > 1:
            raise swat.SWATError(f"CAS staging upload failed: {upload.messages}")
        try:
            appended = self._conn.table.append(
                source={"name": tmp_name, "caslib": self.cfg.caslib},
                target={"name": self.cfg.table_name, "caslib": self.cfg.caslib},
            )
            if appended.severity > 1:
                raise swat.SWATError(f"CAS append failed: {appended.messages}")
        finally:
            self._conn.table.dropTable(
                name=tmp_name, caslib=self.cfg.caslib, quiet=True
            )
        log.info(
            "Batch appended to CAS",
            extra={
                "rows": len(frame),
                "table": self.cfg.table_name,
                "caslib": self.cfg.caslib,
            },
        )

    def _save_table(self) -> None:
        if self._conn is None:
            return
        log.info("Persisting CAS table to caslib storage")
        self._conn.table.save(
            table={"name": self.cfg.table_name, "caslib": self.cfg.caslib},
            name=f"{self.cfg.table_name}.{self.cfg.persist_format}",
            caslib=self.cfg.caslib,
            replace=True,
        )
