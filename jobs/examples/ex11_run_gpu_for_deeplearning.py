"""
Job to showcase using a GPU, for deeplearning in that case, in local and in the cloud.
The jobs runs inferences from ALBERT model for demo purposes.
"""
from yaetos.etl_utils import ETL_Base, Commandliner
from transformers import AlbertTokenizer, TFAlbertForSequenceClassification
from transformers import file_utils
import tensorflow as tf
import numpy as np
import pandas as pd


class Job(ETL_Base):
    MODEL_NAME = 'albert-base-v2'  # or other version of ALBERT

    def transform(self, training_set):
        self.logger.info(training_set)

        # Force TensorFlow to use the CPU
        tf.config.set_visible_devices([], 'GPU')

        self.logger.info(f"Location of huggingface cache model {file_utils.default_cache_path}")
        self.logger.info(f"Tensorflow is_gpu_available: {tf.test.is_gpu_available()}")
        self.logger.info(f"Tensorflow devices: {tf.config.list_physical_devices()}")

        # x_train, y_train, x_test, y_test = self.split_training_data(training_set, 0.8)
        x = training_set['text'].tolist()
        y = training_set['classification'].tolist()

        x_proc = self.preprocess(x)
        model = self.load_model()
        predictions = self.predict(model, x_proc)
        evaluations = pd.DataFrame({'text': x, 'predictions': predictions, 'real': y})
        return evaluations

    def load_model(self):
        model = TFAlbertForSequenceClassification.from_pretrained(self.MODEL_NAME)
        return model

    def split_training_data(self, df, split):
        np.random.seed(42)
        df['training_test'] = np.random.choice(['training', 'test'], size=len(df), p=[split, 1 - split])
        x_train = df[df['training_test'] == 'training']['text'].tolist()
        y_train = df[df['training_test'] == 'training']['classification'].tolist()
        x_test = df[df['training_test'] == 'test']['text'].tolist()
        y_test = df[df['training_test'] == 'test']['classification'].tolist()
        return x_train, y_train, x_test, y_test

    @classmethod
    def preprocess(cls, texts):
        tokenizer = AlbertTokenizer.from_pretrained(cls.MODEL_NAME)
        encoded_inputs = tokenizer(texts, padding=True, truncation=True, max_length=128, return_tensors="tf")
        x = [encoded_inputs['input_ids'], encoded_inputs['attention_mask']]
        return x

    @staticmethod
    def predict(model, x):
        predictions = model.predict(x)
        # The predictions are in logits (raw scores), so we apply a softmax to convert them to probabilities
        probabilities = tf.nn.softmax(predictions.logits, axis=-1).numpy()

        # Take the argmax to get the most likely class
        predicted_classes = np.argmax(probabilities, axis=-1)
        return predicted_classes


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
