#!/usr/bin/env python3
# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check the container-owned context's lifetime and shutdown authority."""

import gc

import _ipc_native
from rclpy.context import Context


def main():
    """Retain the lease independently of the capsule and leave shutdown to C++."""
    native = _ipc_native.Container(False, False, False, '/lease_test')
    lease = native.lease()
    context = Context()
    context.init(context_lease=lease, initialize_logging=False)
    for options in [{'args': []}, {'domain_id': 175}]:
        rejected = Context()
        try:
            rejected.init(context_lease=lease, initialize_logging=False, **options)
        except ValueError:
            pass
        else:
            raise AssertionError('borrowed context accepted replacement initialization')
    del lease
    gc.collect()
    assert context.ok()
    try:
        context.shutdown()
    except RuntimeError as error:
        assert 'container owns context shutdown' in str(error)
    else:
        raise AssertionError('borrower shut down the container context')
    assert context.ok()
    native.close()
    assert not context.ok()
    context.destroy()
    print('Context lease lifetime and shutdown authority passed.')


if __name__ == '__main__':
    main()
