import marimo

__generated_with = "0.17.6"
app = marimo.App(width="medium")


@app.cell
def _():
    from datetime import timedelta

    import marimo as mo
    import polars as pl
    import polars.selectors as cs
    return cs, pl, timedelta


@app.cell
def _(pl, timedelta):
    result = pl.DataFrame(
        [
            (
                1,
                "Scannelatori",
                6,
                timedelta(minutes=10, seconds=26),
                timedelta(minutes=20, seconds=47),
                timedelta(minutes=31, seconds=35),
                timedelta(minutes=41, seconds=13),
            ),
            (
                2,
                "Zu4r",
                6,
                timedelta(minutes=10, seconds=27),
                timedelta(minutes=20, seconds=51),
                timedelta(minutes=31, seconds=43),
                timedelta(minutes=41, seconds=21),
            ),
            (
                3,
                "Bikefitpro",
                6,
                timedelta(minutes=10, seconds=47),
                timedelta(minutes=20, seconds=59),
                timedelta(minutes=32, seconds=00),
                timedelta(minutes=41, seconds=26),
            ),
            (
                4,
                "Dirty",
                6,
                timedelta(minutes=10, seconds=39),
                timedelta(minutes=21, seconds=1),
                timedelta(minutes=32, seconds=7),
                timedelta(minutes=41, seconds=50),
            ),
            (
                5,
                "Kirchmair",
                6,
                timedelta(minutes=10, seconds=37),
                timedelta(minutes=21, seconds=3),
                timedelta(minutes=32, seconds=11),
                timedelta(minutes=41, seconds=56),
            ),
            (
                6,
                "Wattgockel",
                6,
                timedelta(minutes=10, seconds=54),
                timedelta(minutes=21, seconds=32),
                timedelta(minutes=32, seconds=50),
                timedelta(minutes=42, seconds=27),
            ),
            (
                7,
                "Watthletics",
                6,
                timedelta(minutes=10, seconds=55),
                timedelta(minutes=21, seconds=27),
                timedelta(minutes=32, seconds=48),
                timedelta(minutes=42, seconds=42),
            ),
            (
                8,
                "Zsunderrated",
                6,
                timedelta(minutes=10, seconds=44),
                timedelta(minutes=21, seconds=36),
                timedelta(minutes=33, seconds=12),
                timedelta(minutes=43, seconds=22),
            ),
            (
                9,
                "Rc",
                6,
                timedelta(minutes=11, seconds=22),
                timedelta(minutes=22, seconds=12),
                timedelta(minutes=33, seconds=51),
                timedelta(minutes=43, seconds=57),
            ),
            (
                10,
                "Bikes",
                5,
                timedelta(minutes=11, seconds=22),
                timedelta(minutes=22, seconds=20),
                timedelta(minutes=34, seconds=11),
                timedelta(minutes=44, seconds=26),
            ),
            (
                11,
                "Oltre",
                4,
                timedelta(minutes=12, seconds=11),
                timedelta(minutes=22, seconds=54),
                timedelta(minutes=34, seconds=16),
                timedelta(minutes=44, seconds=30),
            ),
        ],
        schema={
            "position": pl.Int8,
            "team": pl.String,
            "n_riders": pl.Int8,
            "split1": pl.Object,
            "split2": pl.Object,
            "split3": pl.Object,
            "total": pl.Object,
        },
        orient="row"
    )
    result
    return (result,)


@app.cell
def _(cs, pl, result):
    (
        result.with_columns(
            cs.starts_with("split")
            .map_elements(lambda td: td.total_seconds(), return_dtype=pl.Float64)
            .rank()
        )
    )
    return


if __name__ == "__main__":
    app.run()
