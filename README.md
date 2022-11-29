# PoC execution instructions

## Dependencies
- Atlas cluster with payment documents
- Atlas CLI (or use Atlas GUI to start/configure cluster)
- Python virtual environment (python 3.8+)
- Test client VMs (or laptop)

### Locust load driver docs
https://locust.io

Main PoC driver file:
`locustpayments.py`

### Install
1. Setup python virtual environment
2. Run pip package install for load driver

    ` pip install -r requirements.txt `

3. If Atlas cluster is not started, un-pause it:

    ` atlas clusters start payments-ods -P se-poc ; atlas clusters watch payments-ods -P se-poc `

4. Edit locust driver for MongoDB cluster URL, and adjust how you want to randomize the keys/fields for the test queries. You can use Atlas CLI to get connection string
    ` atlas clusters connectionStrings describe payments-ods -P se-poc `

5. run single or multiple Locust instances to test

    ` locust -f locustpayments.py `
or ...
5. run locust master and distributed workers

```
> locust -f locustpayments.py --master
> locust -f locustpayments.py --worker --master-host <host IP> --master-port 5557
```
    ==run 1 worker per CPU core on client VM==

6. Open Locust web page to launch performance test
    ` http://<master-host-IP>:8089 `
    - Number of users should be a multiple of workers
    - The faster the query times, the fewer Users necessary to maximize throughput.
    - Users: 48 (3 user instances for 16 distributed workers)
    - Spawn Rate: 8 (users launched per second)

