#!/usr/bin/env python

import specClient as spec


# Plot by spectrum ID
print('get a single ID...')
data = spec.getSpec(2210146812474530816)

print('get a list of IDs...')
data = spec.getSpec([2210146812474530816, 4565636637219987456])


