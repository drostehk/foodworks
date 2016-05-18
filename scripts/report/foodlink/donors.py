# Learn about API authentication here: https://plot.ly/pandas/getting-started
# Find your api_key here: https://plot.ly/settings/api
# Cufflinks binds plotly to pandas dataframes in IPython notebook. Read more

import json
import os
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.plotly as py

from datetime import datetime, timedelta
from math import ceil
from plotly import session, tools, utils
from subprocess import call

class FoodLinkDonorReport(object):
    """FoodLinkDonorReport
    
    Currently only supports FoodLink Donors.

    Requires self.REPORT_FOLDER to be symlinked to FOODWORKS/ 

    Requires wkhtmltopdf to be installed and available on path.
    
    Run from the FoodWorks root folder.

    Example:

    python scripts/report/foodlink/donors.py
    """
    def __init__(self, ngo='FoodLink'):
        super(FoodLinkDonorReport, self).__init__()
        
        self.ROOT_FOLDER = 'data/Canonical/'
        self.REPORT_FOLDER = 'data/Report/' 
        self.ngo = ngo

        self.PERIOD = (datetime.now() - timedelta(days=28))
        self.MONTH_NUM = self.PERIOD.month
        self.MONTH_NAME = self.PERIOD.strftime('%B')
        self.YEAR_NUM = self.PERIOD.year

        self.fn_html = 'temp.html'
        self.fn_pdf  = 'test.pdf'
        self.path_pdf = self.REPORT_FOLDER + ngo + '/' + str(self.YEAR_NUM) + '/' + \
            str(self.MONTH_NUM) + '/'

        self.html_to_pdf_process = [
            "wkhtmltopdf", "--debug-javascript", "--orientation", "Landscape",
            "-T", "5", "-B", "5", "-L", "5", "-R", "5", self.fn_html,
            self.path_pdf + self.fn_pdf ]        

    # PUBLIC

    def data_to_pdf(self):
        data, x, y = self.compose_data([289,234,223,1232],[3290,4279,2167])
        layout = self.compose_layout(x,y)
        fig = go.Figure(data=data, layout=layout)
        html = self.new_iplot(fig)
        self.ensure_dest_exists()
        self.html_to_pdf(html)

    # PRIVATE

    # Plotting

    def new_iplot(self, figure_or_data, show_link=False, link_text='Export to plot.ly',
              validate=True):

        figure = tools.return_figure_from_figure_or_data(figure_or_data, validate)
            
        plotdivid = 'placeholder'
        jdata = json.dumps(figure.get('data', []), cls=utils.PlotlyJSONEncoder)
        jlayout = json.dumps(figure.get('layout', {}), cls=utils.PlotlyJSONEncoder)

        config = {}
        config['showLink'] = show_link
        config['linkText'] = link_text
        jconfig = json.dumps(config)

        plotly_platform_url = session.get_session_config().get('plotly_domain',
                                                               'https://plot.ly')
        if (plotly_platform_url != 'https://plot.ly' and
                link_text == 'Export to plot.ly'):

            link_domain = plotly_platform_url\
                .replace('https://', '')\
                .replace('http://', '')
            link_text = link_text.replace('plot.ly', link_domain)


        script = 'Plotly.plot(gd, {data}, {layout}, {config})'.format(
                data=jdata,
                layout=jlayout,
                config=jconfig)

        html = """
          <head>
          <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.0.0-beta1/jquery.min.js"></script>
          <script type="text/javascript">
        """

        with open('plotly-latest.min.js') as plotly:
            html += plotly.read()

        html += """
                   </script>
             <title>{id}</title>
             <style>
             .logo {{
                position: absolute;
                top:0;
                right:0;
                z-index:100;
                width:20%;
             }}

             g.g-xtitle {{
                transform: translate(0px, 0px)
             }}
             </style>
        </head>
        <body>
            <script type="text/javascript">

            (function() {{
                var d3 = Plotly.d3;

                var WIDTH_IN_PERCENT_OF_PARENT = 100,
                    HEIGHT_IN_PERCENT_OF_PARENT = 100;

                var gd3 = d3.select('body')
                    .append('div')
                    .style({{
                        width: WIDTH_IN_PERCENT_OF_PARENT + '%',
                        'margin-left': (100 - WIDTH_IN_PERCENT_OF_PARENT) / 2 + '%',

                        height: HEIGHT_IN_PERCENT_OF_PARENT + 'vh',
                        'margin-top': (100 - HEIGHT_IN_PERCENT_OF_PARENT) / 2 + 'vh'
                    }});

                var gd = gd3.node();

                {script}

                Plotly.Plots.resize(gd);

                window.onresize = function() {{
                    Plotly.Plots.resize(gd);
                }};

            }})();
                   </script>
                <img class="logo" src="scripts/report/foodlink/logo.jpg">

            </body>
            """.format(id=plotdivid, script=script)

        return html
    
    # Data

    def compose_data(self, weekly_totals, monthly_totals):
        month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        month_styles = ['rgba(204,204,204,1)'] * 12 
        cut = max(len(monthly_totals) - 1, 0)
        base_year = datetime.now().year
        base_week = datetime(base_year, len(monthly_totals), 1).isocalendar()[1]

        for offset, weekly_total in enumerate(weekly_totals):
            monthly_totals.insert(cut+offset, weekly_total)
            month_names.insert(cut + offset, 'WK' + str(base_week + offset))
            month_styles.insert(cut + offset, 'rgba(222,45,38,0.8)')
        trace0 = go.Bar(
            x=month_names,
            y=monthly_totals,
            marker=dict(
                color=month_styles
            )
        )
        data = [trace0]
        return data, month_names, monthly_totals

    # Layout

    def compose_layout(self, x, y):
        layout = go.Layout(
            title='<b>3/3rds, Central</b><br>2016 Weekly Food Donation in KG ',
            titlefont = dict(
                size=22
                ),
            xaxis=dict(
                # set x-axis' labels direction at 45 degree angle
                tickangle=-45,
                title="Number of Meals Provided in APR (Week 14 - Week 17) 2016 : 91",
                titlefont = dict(
                    size=20
                ),
                position = 0
            ),
            showlegend=False,
            autosize=True,
                width= 1340,
                height=920,
                margin=go.Margin(
                    l=0,
                    r=0,
                    b=100,
                    t=40,
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
                ) for xi, yi in zip(x, y)]
            )
        return layout

    # Export

    def html_to_pdf(self, html):

        with open(self.fn_html,'w') as fn:
            fn.write(html)

        call(self.html_to_pdf_process)

    # Utilities 

    def available_csvs(self):
        return os.listdir(self.base_path())
    
    def base_path(self):
        return self.ROOT_FOLDER + self.ngo + '/'

    def ensure_dest_exists(self):
        if not os.path.exists( self.path_pdf):
            os.makedirs( self.path_pdf)

    def week_of_month(self, dt):
        """ Returns the week of the month for the specified date.
        """

        first_day = dt.replace(day=1)

        dom = dt.day
        adjusted_dom = dom + first_day.weekday()

        return int(ceil(adjusted_dom/7.0))


if __name__ == '__main__':
    opts = {
        "ngo" : 'FoodLink',
    }

    report = FoodLinkDonorReport(**opts)
    report.data_to_pdf()