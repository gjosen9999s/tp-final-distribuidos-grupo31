"""Q5 Accumulator: acumula sum/count de <=2020 y emite el parcial."""

import os
from collections import Counter, defaultdict

from common.middleware.group import GroupCoordinator
from common.protocol.constants import (
    CTRL_EOF,
    CTRL_Q5_ACCUMULATOR,
    CTRL_Q5_COUNTER,
    ControlType,
    InternalType,
)
from common.protocol.control import serializer as control
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller


class Q5Accumulator(Controller):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None:
            raise ValueError("Q5 Accumulator necesita OUT_QUEUE")

        # Cantidad de Q5Counter que enviaron DONE a la cola q5_sum
        self.q5_counters_qty = max(1, int(os.getenv("COUNTER_COUNT", "1")))

        # Acumuladores parciales por cliente
        self._partial_words_sum = Counter()
        self._partial_article_qty = Counter()

        # DONE recibidos de Q5Counter por cliente (conjunto de nodeId)
        # y total acumulado de mensajes que se procesaran en esta etapa
        # entre todos los nodos accumulator
        self._received_counter_dones = defaultdict(set)
        self._accumulator_step_msgs = Counter()

        self.group = GroupCoordinator(
            controller_type=CTRL_Q5_ACCUMULATOR,
            node_id=self.node_id,
            progress_batch_size=self.progress_batch_size,
            controller_count=self.controller_count,
            control_out=self.control_out,
            done_sender=self._send_done,
        )

    def _on_data(self, body, ack, nack):
        try:
            if body[0] >= CTRL_EOF:
                msg_type, client_id, message = control.deserialize(body)
                self.handle_control(msg_type, client_id, message, False)
            else:
                msg_type, client_id, message = internal.deserialize(body)
                if msg_type != InternalType.WORDCOUNT_SUM:
                    raise ValueError(
                        f"Mensaje desconocido: {msg_type}"
                    )
                self.handle_article(client_id, message["wordCount"])
            ack()
        except Exception:
            nack()

    def handle_article(self, client_id, word_count):
        # protegido porque se lee desde un hilo y actualiza desde otro
        with self._lock:
            if self.group.is_closed(client_id):
                return

            self._partial_words_sum[client_id] += word_count
            self._partial_article_qty[client_id] += 1

            # cuenta los mensajes procesados por este nodo, siempre
            self.group.on_processed(client_id)

    def handle_control(self, msg_type, client_id, message, from_control):
        with self._lock:
            if self.group.is_closed(client_id):
                return

            if msg_type == ControlType.DONE:
                if message["controllerType"] != CTRL_Q5_COUNTER:
                    return
 
                # El DONE llega desde Q5_Sum y se republica al exchange
                # de control para que lo vean todos los accumulator.
                if not from_control:
                    self.control_out.send(
                        control.serialize_done(
                            client_id,
                            message["controllerType"],
                            message["nodeId"],
                            message["quantity"],
                        )
                    )

                node_id = message["nodeId"]
                if node_id not in self._received_counter_dones[client_id]:
                    self._received_counter_dones[client_id].add(node_id)
                    self._accumulator_step_msgs[client_id] += message["quantity"]

                    # Cuando llegaron los DONE de todos los Counters, ya conocemos
                    # el total exacto de mensajes que debe procesar el grupo de Accumulators.
                    if len(self._received_counter_dones[client_id]) == self.q5_counters_qty:
                        self.group.on_control(
                            ControlType.EOF,
                            client_id,
                            {"total": self._accumulator_step_msgs[client_id]},
                            True,
                        )
                return

            self.group.on_control(msg_type, client_id, message, from_control)

    def _send_done(self, client_id, _quantity):
        # Cada nodo Accumulator emite su parcial (sum, count)
        # y su DONE hacia la cola Q5_Acc_Partial de los Q5_AVG_Filter.
        self.out_mw.send(
            internal.serialize_q5_acc_partial(
                client_id,
                self._partial_words_sum[client_id],
                self._partial_article_qty[client_id],
            )
        )

        self.out_mw.send(
            control.serialize_done(
                client_id,
                CTRL_Q5_ACCUMULATOR,
                self.node_id,
                1,
            )
        )
