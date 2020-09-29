#!/usr/bin/env python

import specClient as spec

from PIL import Image
from io import BytesIO

specobjid = 2210146812474530816
data = BytesIO(spec.preview(specobjid))

image = Image.open(data)
image.show()

