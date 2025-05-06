import mlflow
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

def train_model(data: pd.DataFrame, target_column: str, test_size: float = 0.2):
    """
    Train a machine learning model on the movie data.
    
    Args:
        data: DataFrame containing the training data
        target_column: Name of the target column to predict
        test_size: Proportion of data to use for testing
    
    Returns:
        Trained model and evaluation metrics
    """
    # Split the data
    X = data.drop(columns=[target_column])
    y = data[target_column]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
    
    # Initialize and train the model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Make predictions
    y_pred = model.predict(X_test)
    
    # Calculate metrics
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    # Log metrics with MLflow
    with mlflow.start_run():
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)
        mlflow.sklearn.log_model(model, "model")
    
    return model, {"mse": mse, "r2": r2} 