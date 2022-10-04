library(dplyr)
library(tidyr)

Data = data(cars)

plot(cars$speed,cars$dist)

model= lm(speed ~ dist,data = cars)

summary(model)

line = predict(model)

lines(line,cars$dist)

