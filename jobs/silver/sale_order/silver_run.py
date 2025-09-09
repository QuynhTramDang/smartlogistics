# silver_run.py 

from silver.sale_order.silver_config import get_config_by_job_name
from silver.silver_job_base import FactSilverJob, DimSilverJob

def run_silver_job(job_name: str, batch_id: str = None):
    config = get_config_by_job_name(job_name)
    table_type = config.get("table_type", "fact").lower()
    if table_type == "fact":
        job = FactSilverJob(config=config, batch_id=batch_id)
    elif table_type == "dim":
        job = DimSilverJob(config=config, batch_id=batch_id)
    else:
        raise ValueError(f"Unknown table_type: {table_type}")

    job.run()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python silver_run.py <job_name> [<batch_id>]")
        sys.exit(1)

    job_name = sys.argv[1]
    batch_id = sys.argv[2] if len(sys.argv) > 2 else None
    run_silver_job(job_name, batch_id)
