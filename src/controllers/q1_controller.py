"""Q1: artículos en inglés hasta 2020."""

from dataclasses import dataclass

from common.protocol.constants import (
    CTRL_Q1,
    ControlType,
    FLAG_IS_LAST_PART,
    LANG_EN,
    ResultType,
)
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller


@dataclass
class _ClientState:
    # Estado por cliente. Q1 puede estar procesando varios a la vez.
    total: int | None = None
    processed: int = 0
    last_reported: int = 0
    sum: int = 0
    emitted: int = 0
    flush_mode: bool = False
    flushed: bool = False
    closed: bool = False
    flush_id: int = 0


class Q1Controller(Controller):
    def _state(self, client_id):
        state = self._states.get(client_id)
        if state is None:
            state = _ClientState()
            self._states[client_id] = state
        return state

    def handle_article(self, client_id, article):
        # El lock es porque esto lo llaman dos hilos (de datos y de control).
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            # processed cuenta TODO lo que entra a Q1, matchee o no.
            # emitted cuenta solo lo que sale como resultado.
            state.processed += 1

            if self._matches(article):
                state.emitted += 1
                # Q1 ada artículo que matchea es un resultado completo.
                payload = external.serialize_q1_payload(
                    article.url, article.dateModified
                )
                self.out_mw.send(
                    internal.serialize_result_message(
                        client_id,
                        ResultType.Q1,
                        article.docId,
                        0,
                        FLAG_IS_LAST_PART,
                        payload,
                    )
                )

            # last_reported es lo ultimo reportado, delta = lo que falta reportar.
            # En modo normal espera a juntar B, en modo FLUSH va de a 1.
            delta = state.processed - state.last_reported
            if state.flush_mode:
                if delta > 0:
                    self._send_progress(client_id, delta)
                    state.last_reported = state.processed
            elif delta >= self.progress_batch_size:
                self._send_progress(client_id, delta)
                state.last_reported = state.processed

    def handle_control(self, msg_type, client_id, message, from_control):
        with self._lock:
            state = self._state(client_id)

            if msg_type == ControlType.EOF:
                state.total = message["total"]
                # El EOF se recibe por la cola de datos y lo
                # ve una sola replica que lo envia por la cola de  control.
                # Si from_control es True, ya viene de ese broadcast.
                if not from_control:
                    self.control_out.send(
                        control.serialize_eof(client_id, state.total)
                    )
                self._evaluate(client_id, state)

            elif msg_type == ControlType.PROGRESS:
                # sum es global, cada PROGRESS que llega es de alguna replica o propio.
                state.sum += message["quantity"]
                self._evaluate(client_id, state)

            elif msg_type == ControlType.FLUSH:
                # FLUSH = envia lo pendiente y setea que la proxima vez sea de a 1 msj.
                delta = state.processed - state.last_reported
                if delta > 0:
                    self._send_progress(client_id, delta)
                    state.last_reported = state.processed
                state.flush_mode = True

    def _evaluate(self, client_id, state):
        if state.total is None or state.closed:
            return

        if not state.flushed and state.sum < state.total:
            # Si la suma global esta cerca del total se envia FLUSH. El margen
            # B*N sale de que cada replica puede tener hasta B-1 sin reportar.
            threshold = state.total - self.progress_batch_size * self.controller_count
            if state.sum >= threshold:
                self.control_out.send(
                    control.serialize_flush(client_id, state.flush_id + 1)
                )
                state.flush_id += 1
                state.flushed = True

        if state.sum == state.total:
            # sum == total => entre todas las réplicas se procesó todo.
            # Mando DONE a QueryResults y lo recibe el AH.
            self.out_mw.send(
                control.serialize_done(
                    client_id, CTRL_Q1, self.node_id, state.emitted
                )
            )
            state.closed = True

    def _send_progress(self, client_id, delta):
        # controllerType + nodeId identifican que controller reporta progreso.
        self.control_out.send(
            control.serialize_progress(
                client_id, CTRL_Q1, self.node_id, delta
            )
        )

    @staticmethod
    def _matches(article):
        # dateModified es epoch, usamos el año.
        # Q1 pide inglés y <= 2020.
        if article.inLanguage != LANG_EN:
            return False
        if article.dateModified is None or article.url is None:
            return False
        year = article.year
        return year is not None and year <= 2020
