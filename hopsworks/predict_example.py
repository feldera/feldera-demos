# Deployment predictor uploaded to Hopsworks Model Serving by run.py train.
# When invoked via deployment.predict(inputs=[cc_num]), Hopsworks calls predict()
# with the inputs list. The predictor pulls the latest feature vector for that
# cc_num from the online feature store and runs the trained XGBoost model.

import os
import hsfs
import joblib


class Predict(object):
    def __init__(self):
        """Initialize serving state and load the trained model."""
        fs_conn = hsfs.connection()
        self.fs = fs_conn.get_feature_store()

        # Get the feature view created during training.
        self.fv = self.fs.get_feature_view("transactions_view_streaming_fv", 1)
        self.fv.init_serving(1)

        # Load the trained XGBoost model. Hopsworks Model Serving mounts model
        # files at MODEL_FILES_PATH (typically /mnt/models) and the predictor
        # artifact at ARTIFACT_FILES_PATH (typically /mnt/artifacts). Try both
        # — older deployments bundle everything under ARTIFACT_FILES_PATH.
        pkl = "xgboost_fraud_streaming_model.pkl"
        search = [os.environ.get("MODEL_FILES_PATH"), os.environ.get("ARTIFACT_FILES_PATH")]
        for base in search:
            if not base:
                continue
            candidate = os.path.join(base, pkl)
            if os.path.exists(candidate):
                self.model = joblib.load(candidate)
                break
        else:
            raise FileNotFoundError(
                f"Could not find {pkl} in any of {search}"
            )
        print("Initialization Complete")

    def predict(self, inputs):
        """Serve a prediction request using the trained model."""
        feature_vector = (
            self.fv.get_feature_vector({"cc_num": inputs[0][0]}, return_type="pandas")
            .drop(["date_time"], axis=1)
            .values
        )
        # numpy arrays are not JSON serializable — convert to list.
        return self.model.predict(feature_vector.reshape(1, -1)).tolist()
