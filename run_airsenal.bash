#!/bin/bash
airsenal_update_db
airsenal_run_prediction --num_thread 1
airsenal_run_optimization --num_thread 1
