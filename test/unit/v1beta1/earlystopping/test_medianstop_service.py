# Copyright 2022 The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import multiprocessing
import unittest
from unittest.mock import Mock, patch

import grpc
import grpc_testing
import utils

from pkg.apis.manager.v1beta1.python import api_pb2
from pkg.earlystopping.v1beta1.medianstop.service import MedianStopService


class TestMedianStop(unittest.TestCase):
    def setUp(self):
        # Mock load Kubernetes config.
        patcher = patch('pkg.earlystopping.v1beta1.medianstop.service.config.load_kube_config')
        self.mock_sum = patcher.start()
        self.addCleanup(patcher.stop)

        self.median_stop_service = MedianStopService()
        servicers = {
            api_pb2.DESCRIPTOR.services_by_name['EarlyStopping']: self.median_stop_service
        }

        self.test_server = grpc_testing.server_from_dictionary(
            servicers, grpc_testing.strict_real_time())

    def _prepare_trial_status_update(self):
        trial = {"status": {"conditions": []}}
        get_thread = Mock()
        get_thread.get.return_value = trial
        patch_thread = Mock()

        self.median_stop_service.api_instance = Mock()
        self.median_stop_service.api_instance.get_namespaced_custom_object.return_value = (
            get_thread
        )
        self.median_stop_service.api_instance.patch_namespaced_custom_object_status.return_value = (
            patch_thread
        )

        return trial, patch_thread

    def test_set_trial_status_waits_for_patch(self):
        trial, patch_thread = self._prepare_trial_status_update()

        reply = self.median_stop_service.SetTrialStatus(
            api_pb2.SetTrialStatusRequest(trial_name="test-trial"), None
        )

        self.assertIsInstance(reply, api_pb2.SetTrialStatusReply)
        patch_thread.get.assert_called_once_with(120)
        self.assertEqual(trial["status"]["conditions"][0]["type"], "EarlyStopped")

    def test_set_trial_status_raises_when_patch_fails(self):
        _, patch_thread = self._prepare_trial_status_update()
        patch_thread.get.side_effect = RuntimeError("conflict")

        with self.assertRaisesRegex(
            Exception, "Update status for Trial: test-trial.*conflict"
        ):
            self.median_stop_service.SetTrialStatus(
                api_pb2.SetTrialStatusRequest(trial_name="test-trial"), None
            )

    def test_set_trial_status_raises_when_patch_times_out(self):
        _, patch_thread = self._prepare_trial_status_update()
        patch_thread.get.side_effect = multiprocessing.TimeoutError

        with self.assertRaisesRegex(
            Exception, "Timeout trying to update Katib Trial status"
        ):
            self.median_stop_service.SetTrialStatus(
                api_pb2.SetTrialStatusRequest(trial_name="test-trial"), None
            )

    def test_validate_early_stopping_settings(self):
        # Valid cases
        early_stopping = api_pb2.EarlyStoppingSpec(
            algorithm_name="medianstop",
            algorithm_settings=[
                api_pb2.EarlyStoppingSetting(
                    name="min_trials_required",
                    value="2",
                ),
                api_pb2.EarlyStoppingSetting(
                    name="start_step",
                    value="5",
                ),
            ],
        )

        _, _, code, _ = utils.call_validate(self.test_server, early_stopping)
        self.assertEqual(code, grpc.StatusCode.OK)

        # Invalid cases
        # Unknown algorithm name
        early_stopping = api_pb2.EarlyStoppingSpec(algorithm_name="unknown")

        _, _, code, details = utils.call_validate(self.test_server, early_stopping)
        self.assertEqual(code, grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(details, "unknown algorithm name unknown")

        # Unknown config name
        early_stopping = api_pb2.EarlyStoppingSpec(
            algorithm_name="medianstop",
            algorithm_settings=[
                api_pb2.EarlyStoppingSetting(
                    name="unknown_conf",
                    value="100",
                ),
            ],
        )

        _, _, code, details = utils.call_validate(self.test_server, early_stopping)
        self.assertEqual(code, grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(details, "unknown setting unknown_conf for algorithm medianstop")

        # Wrong min_trials_required
        early_stopping = api_pb2.EarlyStoppingSpec(
            algorithm_name="medianstop",
            algorithm_settings=[
                api_pb2.EarlyStoppingSetting(
                    name="min_trials_required",
                    value="0",
                ),
            ],
        )

        _, _, code, details = utils.call_validate(self.test_server, early_stopping)
        self.assertEqual(code, grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(details, "min_trials_required must be greater than zero (>0)")

        # Wrong start_step
        early_stopping = api_pb2.EarlyStoppingSpec(
            algorithm_name="medianstop",
            algorithm_settings=[
                api_pb2.EarlyStoppingSetting(
                    name="start_step",
                    value="0",
                ),
            ],
        )
        _, _, code, details = utils.call_validate(self.test_server, early_stopping)
        self.assertEqual(code, grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(details, "start_step must be greater or equal than one (>=1)")

    def test_get_earlystopping_rules(self):
        # TODO (andreyvelich): Add more informative tests.
        trials = [
            api_pb2.Trial(
                name="test-asfjh",
            ),
            api_pb2.Trial(
                name="test-234hs",
            )
        ]

        experiment = api_pb2.Experiment(
            name="test",
        )

        request = api_pb2.GetEarlyStoppingRulesRequest(
            experiment=experiment,
            trials=trials,
            db_manager_address="katib-db-manager.kubeflow:6789"
        )

        get_earlystopping_rules = self.test_server.invoke_unary_unary(
            method_descriptor=(api_pb2.DESCRIPTOR
                .services_by_name['EarlyStopping']
                .methods_by_name['GetEarlyStoppingRules']),
            invocation_metadata={},
            request=request, timeout=1)

        _, _, code, _ = get_earlystopping_rules.termination()

        self.assertEqual(code, grpc.StatusCode.OK)


if __name__ == '__main__':
    unittest.main()
