import tecton
from datetime import datetime

def triggerMaterialization():
    fvs = tecton.get_workspace("kz_test").list_feature_views()
    fvs = ["last_distinct96_fv", "last96_fv"]
    for featureView in fvs:
        try:
            fv = tecton.get_workspace("kz_test").get_feature_view(featureView)
            job_id = fv.trigger_materialization_job(
                start_time=datetime(2021, 12, 31),
                end_time=datetime(2024, 12, 31),
                online=True,
                offline=False,
            )
        except Exception as e:
            print(e)
if __name__ == "__main__":
    triggerMaterialization()