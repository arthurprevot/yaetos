"""To show a troubleshooting method, using 'import ipdb; ipdb.set_trace()' below."""
from yaetos.etl_utils import ETL_Base, Commandliner
from transformers import AlbertTokenizer, TFAlbertForSequenceClassification
from transformers import file_utils
import tensorflow as tf
import numpy as np
import pandas as pd



class Job(ETL_Base):
    MODEL_NAME = 'albert-base-v2'  # or other version of ALBERT

    def transform(self, training_set):
        # Force TensorFlow to use the CPU
        tf.config.set_visible_devices([], 'GPU')

        self.logger.info(f"Location of huggingface cache model {file_utils.default_cache_path}")
        self.logger.info(f"Tensorflow is_gpu_available: {tf.test.is_gpu_available()}")
        self.logger.info(f"Tensorflow devices: {tf.config.list_physical_devices()}")

        x_train, y_train, x_test, y_test = self.split_training_data(training_set, 0.8)
        x_test_proc = self.preprocess(x_test)
        model = self.load_model()
        predictions = self.predict(model, x_test_proc)
        evaluations = pd.DataFrame({'text': x_test, 'predictions': predictions, 'real': y_test})

        # model = self.finetune_model(x_train_proc, y_train)
        # path = Path_Handler(self.jargs.output_model['path'], self.jargs.base_path, self.jargs.merged_args.get('root_path')).expand_now(now_dt=self.start_dt)
        # self.save_model(model, path)

        # evaluations = self.evaluate(model, x_test, y_test, x_train, y_train)

        # # Generate a plot of the model's architecture
        # tf.keras.utils.plot_model(model, to_file='model_architecture.png', show_shapes=True)
        return evaluations

    def load_model(self):
        # Load model
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

    # def finetune_model(self, x_train, y_train):
    #     # Load model
    #     model = TFAlbertForSequenceClassification.from_pretrained(self.MODEL_NAME)

    #     # Convert labels to a TensorFlow tensor
    #     y_train = tf.constant(y_train)

    #     # Define loss function and optimizer
    #     loss_function = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
    #     optimizer = tf.keras.optimizers.Adam(learning_rate=5e-5)

    #     # Compile the model
    #     model.compile(optimizer=optimizer, loss=loss_function, metrics=['accuracy'])

    #     # Train the model
    #     model.fit(x=x_train, y=y_train, batch_size=8, epochs=10)
    #     self.logger.info(f"Model summary, post finetuning: {model.summary()}")

    #     return model

    # @staticmethod
    # def save_model(model, path):
    #     model.save_pretrained(path)

    # @staticmethod
    # def reload_model(path):
    #     return TFAlbertForSequenceClassification.from_pretrained(path)

    @staticmethod
    def predict(model, x):
        predictions = model.predict(x)
        # The predictions are in logits (raw scores), so we apply a softmax to convert them to probabilities
        probabilities = tf.nn.softmax(predictions.logits, axis=-1).numpy()

        # Take the argmax to get the most likely class
        predicted_classes = np.argmax(probabilities, axis=-1)
        return predicted_classes

    # def evaluate(self, model, x_test, y_test, x_train, y_train):
    #     x_proc = self.preprocess(x_test)
    #     predictions = self.predict(model, x_proc)
    #     df_test = pd.DataFrame({'text': x_test, 'predictions': predictions, 'real': y_test})
    #     df_test['train_or_test'] = 'test'

    #     x_proc = self.preprocess(x_train)
    #     predictions = self.predict(model, x_proc)
    #     df_train = pd.DataFrame({'text': x_train, 'predictions': predictions, 'real': y_train})
    #     df_train['train_or_test'] = 'train'

    #     return pd.concat([df_test, df_train])


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
