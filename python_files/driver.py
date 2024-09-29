from predict_accident import PredictAccident
from threading import Thread
from buffer import Buffer

if __name__ == '__main__':
	while True:
		Thread(target = Buffer().add_to_buffer()).start()
		Thread(target = PredictAccident().predict_accident()).start()




