

import random
from cubetl import text, flow, fs, script
from cubetl.util import log


def add_random_field(ctx, m):
    m["id"] = text.functions.slugu(m["path"])
    m["fruit"] = random.choice(['apples', 'oranges', 'bananas'])
    m["number"] = random.randint(0, 10)


def cubetl_config(ctx):

    ctx.add('example.directorylist', flow.Chain(steps=[

        log.Log(message='CubETL Example'),

        fs.DirectoryList(path='/'),

        script.Function(add_random_field),

        ctx.get('cubetl.util.print'),

    ]))

    ctx.start_node = 'example.directorylist'

