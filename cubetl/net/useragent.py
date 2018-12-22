
from cubetl.core import Node
from user_agents import parse

import logging

# Get an instance of a logger
logger = logging.getLogger(__name__)


class UserAgentParse(Node):
    """
    Returns platform, os and browser information from common HTTP user agent strings.
    """

    def __init__(self, data='${ m["user_agent_string"] }', result_prefix='ua_'):
        super().__init__()

        self.data = data
        self.result_obj = None  # 'ua'
        self.result_prefix = result_prefix

        self._extract_error = False

    def initialize(self, ctx):

        super(UserAgentParse, self).initialize(ctx)


    def process(self, ctx, m):

        ua_string = ctx.interpolate(m, self.data)
        user_agent = parse(ua_string)

        if (self.result_obj):
            m[self.result_obj] = user_agent

        if (self.result_prefix):
            m[self.result_prefix + 'user_agent_family'] = user_agent.browser.family
            m[self.result_prefix + 'user_agent_version_string'] = user_agent.browser.version_string
            m[self.result_prefix + 'os_family'] = user_agent.os.family
            m[self.result_prefix + 'os_version_string'] = user_agent.os.version_string
            m[self.result_prefix + 'device_family'] = user_agent.device.family
            m[self.result_prefix + 'is_mobile'] = user_agent.is_mobile
            m[self.result_prefix + 'is_tablet'] = user_agent.is_tablet
            m[self.result_prefix + 'is_pc'] = user_agent.is_pc
            m[self.result_prefix + 'is_bot'] = user_agent.is_bot

        yield m

