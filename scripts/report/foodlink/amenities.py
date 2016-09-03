#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function)

import plotly.graph_objs as go

try:
    from donors import FoodLinkDonorReport
except ImportError:
    import sys, os
    sys.path.append( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) )
    from foodlink.donors import FoodLinkDonorReport


class FoodLinkAmenitiesReport(FoodLinkDonorReport):
    """FoodLinkAmenitiesReport

    Currently only supports FoodLink Donors.

    Requires self.REPORT_FOLDER/{ngo} to be symlinked to FOODWORKS/Reports/{ngo}

    Requires wkhtmltopdf to be installed and available on path.

    Run from the FoodWorks root folder.

    Example:

    python scripts/report/foodlink/amenities.py
    """

    def __init__(self, ngo='FoodLink'):
        super(FoodLinkAmenitiesReport, self).__init__()

        self.pickup_type = 'amenities'

    # PRIVATE

    # Data

    def compose_layout(self, x, y, weeks, opts):
        layout = go.Layout(
            title='<b>{name}</b><br>{year} Weekly Amenities Donation in KG '.format(**opts),
            titlefont=dict(
                size=28
            ),
            xaxis=dict(
                # set x-axis' labels direction at 45 degree angle
                # tickangle=-45,
                tickfont=dict(
                    size=20
                ),
                titlefont=dict(
                    size=24
                ),
                position=0
            ),
            showlegend=False,
            autosize=True,
            width=1340,
            height=920,
            margin=go.Margin(
                l=0,
                r=0,
                b=100,
                t=80,
                pad=100
            ),
            annotations=[
                dict(
                    x=xi,
                    y=yi,
                    text=str(yi),
                    xanchor='center',
                    yanchor='bottom',
                    showarrow=False,
                    font=dict(
                        size=18,
                    )
                ) for xi, yi in zip(x, y)]
        )
        return layout

    # Export

    def relevant_csvs(self):
        return [fn for fn in self.available_csvs() if
                ".".join([str(self.YEAR_NUM), 'collection - Amenities']) in fn]

if __name__ == '__main__':
    opts = {
        "ngo": 'FoodLink',
    }
    report = FoodLinkAmenitiesReport(**opts)
    report.data_to_pdf()
