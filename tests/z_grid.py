#!/usr/bin/env python

import specClient as spec
from PIL import Image
from io import BytesIO



ids = [2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
       2210146812474530816,
      ]


data = spec.plotGrid(ids, 2, 2, page=2)

image = Image.open(data)
image.show()

