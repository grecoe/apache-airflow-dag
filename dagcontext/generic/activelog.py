# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

from datetime import datetime
import json
import os
import sys

class ActivityLog:
    """
    Class to be used across task instances to keep an overhead log in 

    /tmp/activity_log 

    Logs are tagged with the run ID 
    """
    ACTIVITY_LOG_DIRECTORY = None
    ACTIVITY_LOG_BASETASK_ID = None
    ACTIVITY_LOG_CACHE_SIZE = 50

    @staticmethod
    def log_segment(segment_name):
        """Only to file if present deliniate between tasks"""
        output_content = []
        output_content.append("{} ************************************************".format(
            ActivityLog._get_timestamp()
            )
        )
        output_content.append("{} {}".format(ActivityLog._get_timestamp(), segment_name))
        output_content.append("{} ************************************************".format(
            ActivityLog._get_timestamp()
            )
        )

        # Dump to archive file
        ActivityLog._output_to_activity_log(output_content)

    @staticmethod
    def log_info(*args):
        """Informational logging"""
        ActivityLog._log_something("INFO", *args)

    @staticmethod
    def log_warning(*args):
        """Warning logging"""
        ActivityLog._log_something("WARN", *args)

    @staticmethod
    def log_error(*args):
        """ Error Logging"""
        ActivityLog._log_something("ERROR", *args)

    @staticmethod
    def _log_something(level, *args):
        """
        Breaks down the args

        - Dict/List dumped as JSON
        - Exceptions log file and line with exception
        - Strings are straight out
        - Other are string versions of whatever it is.
        """
        lines = []

        for arg in args:
            output = arg
            if isinstance(output, Exception):
                exception_type, exception_object, exception_traceback = sys.exc_info()
                filename = exception_traceback.tb_frame.f_code.co_filename
                filename = os.path.split(filename)[1]
                line_number = exception_traceback.tb_lineno
                lines.append("({}:{}) {}".format(filename, line_number, exception_type))
                lines.append("\t{}".format(str(output)))

            elif isinstance(output, dict) or isinstance(output, list):
                result = json.dumps(output, indent=4)
                individuals = result.split("\n")
                lines.extend(individuals)

            elif not isinstance(output, str):
                lines.append(str(output))

            else:
                lines.append(output)

        # Dump to console, i.e. Airflow log
        output_content = []
        for line in lines:
            output_content.append("{} {:<8} {}".format(ActivityLog._get_timestamp(), level, line))
            print(line)

        # Dump to archive file
        ActivityLog._output_to_activity_log(output_content)

    @staticmethod
    def _get_timestamp() -> str:
        """Get timestamp for log entry"""
        now = datetime.utcnow()
        return now.strftime("%m/%d/%Y - %H:%M:%S:")

    @staticmethod
    def _output_to_activity_log(lines):
        """Dumps the lines generated out to the appropriate log"""
        if ActivityLog.ACTIVITY_LOG_BASETASK_ID and ActivityLog.ACTIVITY_LOG_DIRECTORY:
            if not os.path.exists(ActivityLog.ACTIVITY_LOG_DIRECTORY):
                os.makedirs(ActivityLog.ACTIVITY_LOG_DIRECTORY)
            
            file_name = "{}_activity.log".format(ActivityLog.ACTIVITY_LOG_BASETASK_ID)
            file_path = os.path.join(ActivityLog.ACTIVITY_LOG_DIRECTORY, file_name)

            with open(file_path,"a") as activity_output:
                for line in lines:
                    activity_output.write("{}\n".format(line))

        # Maintain archive to size specified.
        ActivityLog._maintain_archive()

    @staticmethod
    def _maintain_archive(archive_size = ACTIVITY_LOG_CACHE_SIZE):
        """Used to clear out old logs so we don't overflow the system"""
        if ActivityLog.ACTIVITY_LOG_DIRECTORY:
            found_files = {}
            if os.path.exists(ActivityLog.ACTIVITY_LOG_DIRECTORY):
                for root, dirs, files in os.walk(ActivityLog.ACTIVITY_LOG_DIRECTORY, topdown=False):
                    for name in files:
                        file_path = os.path.join(root, name)
                        file_time = os.path.getctime(file_path)
                        found_files[file_time] = file_path

            keys = list(found_files.keys())
            # Sort oldest to last
            keys.sort()
            if len(keys) > archive_size:
                clean_count = archive_size - len(keys)
                current_count = 0
                for key in keys:
                    print("Clearing old file", found_files[key])
                    os.remove(found_files[key])
                    current_count += 1

                    if current_count >= clean_count:
                        break

"""
ActivityLog.ACTIVITY_LOG_DIRECTORY = "./activity_logs"
ActivityLog.ACTIVITY_LOG_BASETASK_ID = "TEST_ID4"
ActivityLog.log_info("Generic message")
ActivityLog.log_info("API RETURN VALUE", {"first" : "value", "second" : "value2"})
try:
    raise Exception("oops")
except Exception as ex:
    ActivityLog.log_error("Failed to acaquire SP", ex)
"""