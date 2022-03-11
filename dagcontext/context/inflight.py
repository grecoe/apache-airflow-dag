# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

import os
import datetime
from pprint import pprint


class InflightTracker:
    """
    With multiple DAGS able to run at the same time conflicts can arise where N DAGS attempt to 
    process the same file, where N>1. In this case we can get multiple duplicates of the results
    put into OAK. 

    This class creates an "inflight" file locally for each instance of a running DAG using the run
    ID in the name. 

    That file tracks which files are being processed as well as a file with the file ID placed into 
    the same local directory containing only a time stamp. 

    Other processes can determine if a file it thinks it should process is being processed by another
    instance of the DAG. If it is, it should be ignored to prevent duplicate processing.
    """
    def __init__(self, task_run_id:str, inflight_path:str):
        """
        Constructor 

        Parameters:
        task_run_id: Run id of this instantiation of the DAG
        inflight_path: Folder in which to track inflight information.
        """
        self.task_run_id = task_run_id

        # Make sure path exists
        self.inflight_path = inflight_path
        self.processing_path = os.path.join(inflight_path, "processing-{}.txt".format(self.task_run_id))
        if not os.path.exists(self.inflight_path):
            os.makedirs(self.inflight_path)

    def inflight_exists(self, file_id:str) -> bool:
        """
        To prevent multiiple invocation on the same file being pushed to OAK when multiple workflows
        are executed simutaneously, "inflight" records are created so follow on workflows don't pick 
        up the same file. 

        This part of that process is to determine if the file is already being processed by another
        workflow. 

        Parameters:
        file_id: OAK File id being processed

        Returns:
        If the file_id is being managed by a different workflow  
        """
        return_value = False
        if file_id:
            record = os.path.join(self.inflight_path, file_id.split(':')[-1])
            return_value = os.path.exists(record)


        return return_value

    def inflight_append(self, file_ids:list) -> None:
        """
        To prevent multiiple invocation on the same file being pushed to OAK when multiple workflows
        are executed simutaneously, "inflight" records are created so follow on workflows don't pick 
        up the same file. 

        This part of that process is to add a file to the inflight directory to let other workflows
        know that the file is being managed already. 

        Parameters:
        file_ids: A list of records to generate a file for, each being an OAK File
        """
        if file_ids:
            # Track them in case of error
            self._append_process_records(file_ids)
            for id in file_ids:
                record = os.path.join(self.inflight_path, id.split(':')[-1])
                with open(record, "w") as record_file:
                    record_file.writelines(str(datetime.datetime.now()))

    def infligth_remove(self, file_ids:list) -> int:
        """
        To prevent multiiple invocation on the same file being pushed to OAK when multiple workflows
        are executed simutaneously, "inflight" records are created so follow on workflows don't pick 
        up the same file. 

        This part of that process is to remove files associated with a specific record because it has 
        already been processed and cleared from storage.  

        Parameters:
        file_ids: A list of records to remove the file for, each being an OAK File
        """

        remove_count = 0
        if file_ids:
            for id in file_ids:
                record = os.path.join(self.inflight_path, id.split(':')[-1])
                if os.path.exists(record):
                    print("Deleting record inflight record:", record)
                    try:
                        remove_count+=1
                        os.remove(record)
                    except FileNotFoundError:
                        # As much as we try and avoid this there are too many race 
                        # conditions where it was valid right before it wasn't. 
                        # So ignore it because it's gone anyway. 
                        pass
                else:
                    print(record, "inflight file is not currently present")
        
        return remove_count

    def abandon(self, error = None) -> int:
        """
        Either at the end of a succeful execution OR after some records were pinned but 
        the process terminated in error, the data in the inflight folder needs to be cleared. 

        This process removes all of the files associated with this instance as well as the 
        parent record. 
        """
        print("Inflight Tracking Abandoned: ", str(error) if error else "NO ERRORS")

        return_value = 0
        if os.path.exists(self.processing_path):
            tracked_records = []
            with open(self.processing_path, "r") as proc:
                tracked_records = proc.readlines()

            if tracked_records:
                tracked_records = [x.strip() for x in tracked_records]
                return_value = self.infligth_remove(tracked_records)

            os.remove(self.processing_path)  

        return return_value

    def _append_process_records(self, file_ids:list) -> None:
        """
        Internal function to add in ID's to the main file which tracks records
        for this instance. 
        """
        if file_ids:
            with open(self.processing_path, "a") as proc:
                for id in file_ids:
                    proc.writelines(id + "\n")

