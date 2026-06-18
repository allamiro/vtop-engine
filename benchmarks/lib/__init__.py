"""VTOP benchmark framework — shared library.

Kept deliberately separate from the engine: this package only *drives* the
compiled `vtopctl` binary, generates seed data, and collects metrics. It never
imports or links engine code.
"""
