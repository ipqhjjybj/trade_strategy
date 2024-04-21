

class FreqtradeException(Exception):
    """
    Freqtrade base exception. Handled at the outermost level.
    All other exception types are subclasses of this exception type.
    """


class StrategyError(FreqtradeException):
    """
    Errors with custom user-code detected.
    Usually caused by errors in the strategy.
    """


class OperationalException(FreqtradeException):
    """
    Requires manual intervention and will stop the bot.
    Most of the time, this is caused by an invalid Configuration.
    """
