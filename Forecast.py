import numpy as np
from matplotlib import pyplot as plt


def forecast(price, mean, sd, days, sample, strike):
    exercised = 0
    y = [[]]
    for i in range(sample):
        latestReturn = np.random.normal(mean, sd)
        latestPrice = round(price * (1 + latestReturn), 2)
        prices = [latestPrice]
        for n in range(days - 1):
            latestReturn = np.random.normal(mean, sd)
            latestPrice = round(latestPrice * (1 + latestReturn), 2)
            prices.append(latestPrice)
        if any(i < strike for i in prices):
            exercised += 1
        y.append(prices)
    del y[0]
    return y, exercised / sample


y, probability = forecast(41.95, 0.00050814, 0.022468102, 12, 10000, 36)
print(probability)
for prices in y:
    plt.plot(range(12), prices)
plt.show()
