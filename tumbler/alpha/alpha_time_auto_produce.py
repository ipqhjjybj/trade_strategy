# coding=utf-8

import operator
import time
import random
from copy import copy

import talib
from deap import gp, creator, base, tools, algorithms

from tumbler.function.technique import PD_Technique
from tumbler.service import log_service_manager

from .basic_func import *
from .alpha_template import AlphaTemplate
from .alpha_estimate import AlphaEstimate


class AlphaTimeAutoProduce(AlphaTemplate):
    """
    通过时间序列的方式， 计算指标的， 然后如果 > 1 时则开多， < -1时则开空
    """

    def __init__(self, df, population_num=300, canshu_nums=0, name="auto", out_file_name="time_out.csv"):
        super(AlphaTimeAutoProduce, self).__init__(df)

        self._population_num = population_num
        self._name = name
        self._rate_name = "rate_{}".format(1)
        self._df = df
        self._canshu_nums = canshu_nums
        self._df = PD_Technique.rate(self._df, 1, field="close", name=self._rate_name)

        self.pset = self.init_primitive_set()
        self.toolbox = self.init_toolbox()

        self.out_f = open(out_file_name, "w")

    def evalfunc(self, individual):
        try:
            code = str(individual)
            tdf = copy(self._df)
            tdf[self._name] = numpy_standardize(numpy_winsorize(self.toolbox.compile(expr=individual)))
            score, sharpe, trade_times = AlphaEstimate.estimate_time_alpha(tdf, code, self._name)
            if score > 0.1 and sharpe > 0.3:
                print(code, score, sharpe, trade_times, "\n", tdf[self._name])
                if trade_times > 100:
                    self.out_f.write("{},{},{},{}\n".format(code, score, sharpe, trade_times))
                    self.out_f.flush()
            if trade_times <= 100:
                score = 0
            return sharpe, score,
        except Exception as ex:
            log_service_manager.write_log("ex:{}".format(ex))
            return 0, 0,

    def init_primitive_set(self):
        pset = gp.PrimitiveSet("MAIN", self._canshu_nums)
        pset.addPrimitive(rank, 1)
        for window in range(5, 80, 5):
            pset.addPrimitive(partial(talib.MA, timeperiod=window, matype=0), 1, name="MA_{}".format(window))
            pset.addPrimitive(partial(talib.EMA, timeperiod=window), 1, name="EMA_{}".format(window))
            pset.addPrimitive(partial(talib.ROC, timeperiod=window), 1, name="ROC_{}".format(window))
            pset.addPrimitive(partial(talib.RSI, timeperiod=window), 1, name="RSI_{}".format(window))
            pset.addPrimitive(partial(talib.MAX, timeperiod=window), 1, name="MAX_{}".format(window))
            pset.addPrimitive(partial(talib.MIN, timeperiod=window), 1, name="MIN_{}".format(window))
            pset.addPrimitive(partial(talib.STDDEV, timeperiod=window), 1, name="STDDEV_{}".format(window))
            pset.addPrimitive(np.add, 2, name="vadd")
            pset.addPrimitive(np.subtract, 2, name="vsub")
            pset.addPrimitive(np.multiply, 2, name="vmul")
            pset.addPrimitive(np.divide, 2, name="vdiv")
            pset.addPrimitive(np.negative, 1, name="vneg")
            # pset.addPrimitive(np.cos, 1, name="vcos")
            # pset.addPrimitive(np.sin, 1, name="vsin")

        pset.addTerminal(self.open, "open")
        pset.addTerminal(self.high, "high")
        pset.addTerminal(self.low, "low")
        pset.addTerminal(self.close, "close")
        pset.addTerminal(self.volume, "volume")
        pset.addTerminal(self.returns, "returns")
        return pset

    def init_toolbox(self):
        creator.create("FitnessAssume", base.Fitness, weights=(1.0, 1.0))
        creator.create("Individual", gp.PrimitiveTree, fitness=creator.FitnessAssume)

        toolbox = base.Toolbox()
        toolbox.register("expr", gp.genHalfAndHalf, pset=self.pset, min_=1, max_=5)
        toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.expr)
        toolbox.register("population", tools.initRepeat, list, toolbox.individual)
        toolbox.register("compile", gp.compile, pset=self.pset)

        toolbox.register("evaluate", self.evalfunc)
        toolbox.register("select", tools.selTournament, tournsize=3)
        toolbox.register("mate", gp.cxOnePoint)
        toolbox.register("expr_mut", gp.genFull, min_=0, max_=2)
        toolbox.register("mutate", gp.mutUniform, expr=toolbox.expr_mut, pset=self.pset)

        toolbox.decorate("mate", gp.staticLimit(key=operator.attrgetter("height"), max_value=17))
        toolbox.decorate("mutate", gp.staticLimit(key=operator.attrgetter("height"), max_value=17))
        return toolbox

    def run(self, cxpb=0.5, mutpb=0.1, ngen=40):
        random.seed(int(time.time()))

        pop = self.toolbox.population(n=self._population_num)
        hof = tools.HallOfFame(1)

        stats_fit = tools.Statistics(lambda ind: ind.fitness.values)
        stats_size = tools.Statistics(len)
        mstats = tools.MultiStatistics(fitness=stats_fit, size=stats_size)
        mstats.register("avg", np.mean)
        mstats.register("std", np.std)
        mstats.register("min", np.min)
        mstats.register("max", np.max)

        pop, log = algorithms.eaSimple(pop, self.toolbox, cxpb, mutpb, ngen, stats=mstats,
                                       halloffame=hof, verbose=True)
        # print log
        return pop, log, hof
