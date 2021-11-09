#!/usr/bin/python3
"""
  (C) Copyright 2018-2021 Intel Corporation.

  SPDX-License-Identifier: BSD-2-Clause-Patent
"""
import os
import re
import time

from ior_test_base import IorTestBase
from mdtest_test_base import MdtestBase
from logger_utils import TestLogger
from general_utils import get_subprocess_stdout
from ior_utils import IorMetrics

# TODO separate client server logs as Phil's example

class PerformanceTestBase(IorTestBase, MdtestBase):
    # pylint: disable=too-many-ancestors
    """Base performance class."""

    def __init__(self, *args, **kwargs):
        """Initialize a PerformanceTestBase object."""
        super().__init__(*args, **kwargs)

    def setUp(self):
        """Set up each test case."""
        # Start the servers and agents
        super().setUp()

        # TODO python logging.Logger is not flexible enough
        # self.output_performance_dir = os.path.join(self.outputdir, "performance")
        # self.performance_log = TestLogger(self.output_performance_dir, None)
        # print("HERE")
        # print(self.log.log)
        # self.performance_log.info("HERE")
        # TODO proper
        self.output_performance_log = open(os.path.join(self.outputdir, "performance.log"), "w+")

    def pre_tear_down(self):
        # TODO proper
        self.output_performance_log.close()
        return []

    def log_performance(self, msg):
        """Log a performance-related message to self.log.info and self.output_performance_log.

        Args:
            msg (str): the message.

        """
        self.log.info(msg)
        self.output_performance_log.write(msg + "\n")
        self.output_performance_log.flush() # TODO too heavy?

    @property
    def variant_id(self):
        return "{}-{}".format(self.job_id, os.getpid())

    def print_performance_params(self, cmd, extra_params=[]):
        """Print performance parameters.

        Args:
            cmd (str): ior or mdtest
            extra_params (list, optional): extra params to print

        """
        # Start with common parameters
        # Build a list of [PARAM_NAME, PARAM_VALUE]
        # TODO print number of engines
        params = [
            ["TEST_ID", self.variant_id],
            ["TEST_NAME", self.test_id],
            ["NUM_SERVERS", len(self.hostlist_servers)],
            ["NUM_CLIENTS", len(self.hostlist_clients)],
            ["PPC", self.ppn],
            ["PPN", self.ppn]
        ]

        # Get ior/mdtest specific parameters
        cmd = cmd.lower()
        if cmd == "ior":
            params += [
                ["API", self.ior_cmd.api.value],
                ["OCLASS", self.ior_cmd.dfs_oclass.value],
                ["XFER_SIZE", self.ior_cmd.transfer_size.value],
                ["BLOCK_SIZE", self.ior_cmd.block_size.value],
                ["SW_TIME", self.ior_cmd.sw_deadline.value],
                ["CHUNK_SIZE", self.ior_cmd.dfs_chunk.value]
            ]
        elif cmd == "mdtest":
            params += [
                ["API", self.mdtest_cmd.api.value],
                ["OCLASS", self.mdtest_cmd.dfs_oclass.value],
                ["DIR_OCLASS", self.mdtest_cmd.dfs_dir_oclass.value],
                ["SW_TIME", self.mdtest_cmd.stonewall_timer.value],
                ["CHUNK_SIZE", self.mdtest_cmd.dfs_chunk.value]
            ]
        else:
            self.fail("Invalid cmd: {}".format(cmd))

        # Add the extra params
        params += extra_params

        # Print and align all parameters in the format:
        # PARAM_NAME : PARAM_VALUE
        max_len = max([len(param[0]) for param in params])
        for param in params:
            self.log_performance("{:<{}} : {}".format(param[0], max_len, param[1]))

    def print_system_status(self):
        """TODO. Similar to Frontera. Check where ior/mdtest already print some """
        # maybe scan_info = self.dmg.system_query(verbose=True) and dmg_utils.check_system_query_status
        pass

    def verify_oclass_compat(self, oclass):
        """Verify an object class is compatible with the number of servers.

        TODO move this to a lower level in the framework.

        Args:
            oclass (str): The object class. Assumed to be valid.

        """
        patterns = [
            "EC_([0-9]+)P([0-9])+",
            "RP_([0-9]+)"
        ]
        # TODO support for ongoing work with -hosts
        available_servers = len(self.hostlist_servers)
        for pattern in patterns:
            # Findall returns a list where each element is a tuple of groups ()
            match = re.findall(pattern, oclass)
            if match:
                # Sum all groups (). Only index 0 should exist.
                min_servers = sum(int(n) for n in match[0])
                if available_servers < min_servers:
                    self.fail("Need at least {} servers for oclass {}".format(min_servers, oclass))
                break

    def run_performance_ior(self, namespace=None, use_intercept=True, kill_delay_write=None, kill_delay_read=None):
        """Run an IOR performance test.

        Write and Read are ran separately.

        Args:
            namespace (str, optional): namespace for IOR parameters in the yaml.
                Defaulits to None, which uses default IOR namespace.
            use_intercept (bool, optional): whether to use the interception library with dfuse.
                Defaults to True.
            kill_delay_write (float, optional): fraction of stonewall time after which to kill a
                rank during write phase. Must be between 0 and 1. Default is None.
            kill_delay_read (float, optional): fraction of stonewall time after which to kill a
                rank during read phase. Must be between 0 and 1. Default is None.

        """
        if kill_delay_write is not None and (kill_delay_write < 0 or kill_delay_write > 1):
            self.fail("kill_delay_write must be between 0 and 1")
        if kill_delay_read is not None and (kill_delay_read < 0 or kill_delay_read > 1):
            self.fail("kill_delay_read must be between 0 and 1")

        # Always get processes and ppn from the default ior namespace.
        # Needed to avoid conflict between ior and mdtest test bases.
        self.processes = self.params.get("np", '/run/ior/client_processes/*')
        self.ppn = self.params.get("ppn", '/run/ior/client_processes/*')

        if use_intercept:
            # TODO path should really be abstracted to a function, etc.
            intercept = os.path.join(self.prefix, 'lib64', 'libioil.so')
        else:
            intercept = None

        if namespace is not None:
            self.ior_cmd.namespace = namespace
            self.ior_cmd.get_params(self)

        # Calculate both kill delays upfront since read phase will remove stonewall
        kill_after_write = (kill_delay_write or 0) * (self.ior_cmd.sw_deadline.value or 0)
        kill_after_read = (kill_delay_read or 0) * (self.ior_cmd.sw_deadline.value or 0)

        write_flags = self.params.get("write_flags", self.ior_cmd.namespace)
        read_flags = self.params.get("read_flags", self.ior_cmd.namespace)
        if write_flags is None:
            self.fail("write_flags not found in config")
        if read_flags is None:
            self.fail("read_flags not found in config")

        self.print_performance_params(
            "ior",
            [["IOR Write Flags", write_flags],
             ["IOR Read Flags", read_flags]])

        self.verify_oclass_compat(self.ior_cmd.dfs_oclass.value)

        # Create pool and container upfront so fault injection timing is more accurate
        self.update_ior_cmd_with_pool()

        self.subprocess = True

        self.log.info("Running IOR write")
        self.ior_cmd.flags.update(write_flags)
        self.run_ior_with_pool(
            create_pool=False,
            create_cont=False,
            intercept=intercept,
            intercept_info=False,
            stop_dfuse=True # Stop so read has a cold start
        )
        self.check_subprocess_status("write")
        if kill_after_write:
            # TODO test this
            time.sleep(kill_after_write)
            rank_to_kill = len(self.hostlist_servers) - 1
            self.server_managers[0].stop_ranks([rank_to_kill], self.d_log, force=True)
        self.job_manager.process.wait()
        ior_write_output = get_subprocess_stdout(self.job_manager.process)
        ior_write_metrics = self.ior_cmd.get_ior_metrics(ior_write_output)
        self.log_performance("Max Write: {}".format(ior_write_metrics[0][IorMetrics.Max_MiB]))

        self.log.info("Running IOR read")
        self.ior_cmd.flags.update(read_flags)
        self.ior_cmd.sw_wearout.update(None)
        self.ior_cmd.sw_deadline.update(None)
        self.run_ior_with_pool(
            create_pool=False,
            create_cont=False,
            intercept=intercept,
            intercept_info=False,
            stop_dfuse=True
        )
        self.check_subprocess_status("read")
        if kill_after_read:
            # TODO test this
            time.sleep(kill_after_read)
            rank_to_kill = len(self.hostlist_servers) - 2
            self.server_managers[0].stop_ranks([rank_to_kill], self.d_log, force=True)
        self.job_manager.process.wait()
        ior_read_output = get_subprocess_stdout(self.job_manager.process)
        ior_read_metrics = self.ior_cmd.get_ior_metrics(ior_read_output)
        self.log_performance("Max Read: {}".format(ior_read_metrics[0][IorMetrics.Max_MiB]))

    def run_performance_mdtest(self, namespace=None):
        """Run an MdTest performance test.

        Args:
            namespace (str, optional): namespace for MdTest parameters in the yaml.
                Defaulits to None, which uses default MdTest namespace.

        """
        # Always get processes and ppn from the default mdtest namespace.
        # Needed to avoid conflict between ior and mdtest test bases.
        self.processes = self.params.get("np", '/run/mdtest/client_processes/*')
        self.ppn = self.params.get("ppn", '/run/mdtest/client_processes/*')

        if namespace is not None:
            self.mdtest_cmd.namespace = namespace
            self.mdtest_cmd.get_params(self)

        self.print_performance_params("mdtest")

        self.verify_oclass_compat(self.mdtest_cmd.dfs_oclass.value)
        self.verify_oclass_compat(self.mdtest_cmd.dfs_dir_oclass.value)

        self.log.info("Running MDTEST")
        self.execute_mdtest()
        # TODO print easy-to-parse metrics
