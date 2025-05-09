#!/bin/bash

python src/train/train.py train --dataset combine --dataset_path ./data --model rf > rf_training.out
python src/train/train.py train --dataset combine --dataset_path ./data --model xg > xg_training.out
python src/train/train.py train --dataset combine --dataset_path ./data --model gbm > gbm_training.out
