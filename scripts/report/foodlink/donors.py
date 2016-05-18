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

# TODO : Adding in previous month's data
# TODO : Handle if there's isn't a pickup every week of the month
# TODO : Add Week-Range in the Subtitle
# TODO : Clear Temp Files
# TODO : Split off HTML Template

class FoodLinkDonorReport(object):
    """FoodLinkDonorReport
    
    Currently only supports FoodLink Donors.

    Requires self.REPORT_FOLDER/{ngo} to be symlinked to FOODWORKS/Reports/{ngo}

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

        # Dates

        self.PERIOD = (datetime.now() - timedelta(days=28))
        self.MONTH_NUM = self.PERIOD.month
        self.MONTH_NAME = self.PERIOD.strftime('%B')
        self.YEAR_NUM = self.PERIOD.year

        self.month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        self.month_styles = ['rgba(204,204,204,1)'] * 12 
        self.weekly_style = 'rgba(222,45,38,0.8)'

        # Filenames
        
        self.fn = ''
        self.fn_html = self.fn + '.html'
        self.fn_pdf  = self.fn + '.pdf'
        self.path_pdf = self.REPORT_FOLDER + ngo + '/' + str(self.YEAR_NUM) + '/' + \
            str(self.MONTH_NUM) + '/'

        self.html_to_pdf_process = [
            "wkhtmltopdf", "--debug-javascript", "--orientation", "Landscape",
            "-T", "5", "-B", "5", "-L", "5", "-R", "5", self.fn_html,
            self.path_pdf + self.fn_pdf ] 



    # PUBLIC

    def data_to_pdf(self):
        df = self.prepare_data()
        for donor in df.index.unique():
            self.set_fns(donor)
            print(donor)
            weekly_totals = df.ix[donor,:].value.tolist()
            print(weekly_totals)
            try:
                monthly_totals = sum(weekly_totals)
            except TypeError:
                monthly_totals = weekly_totals
                weekly_totals = [weekly_totals]
            print(monthly_totals)
            data, x, y = self.compose_data(weekly_totals,[0,0,0,monthly_totals])
            layout = self.compose_layout(x,y,len(weekly_totals),self.opts(df,donor))
            fig = go.Figure(data=data, layout=layout)
            html = self.new_iplot(fig)
            self.ensure_dest_exists()
            self.html_to_pdf(html)

        self.clean_temp_files()

    def opts(self, df, donor):
        return dict(
            name = df.ix[donor,'name'][0]
            month = 'APR',
            week_s = 14,
            week_e = 17,
            year = self.YEAR_NUM,
            meals = 96
        )

    def set_fns(self,donor):
        self.fn = "{}.{}.{}.{}.report".format(self.ngo, donor, self.YEAR_NUM, self.MONTH_NUM)
        self.fn_html = self.fn + '.html'
        self.fn_pdf = self.fn + '.pdf'
        self.html_to_pdf_process = [
            "wkhtmltopdf", "--debug-javascript", "--orientation", "Landscape",
            "-T", "5", "-B", "5", "-L", "5", "-R", "5", self.fn_html,
            self.path_pdf + self.fn_pdf ] 

    # PRIVATE

    # Data Wrangling

    def prepare_data(self):
        df = pd.concat([pd.read_csv(self.ROOT_FOLDER + self.ngo + '/' + fx) for
            fx in self.relevant_csvs()]).fillna(0)
        df = self.merge_donors(df)
        efficiency = self.split_off_agg_column(df,'donor','efficiency')
        names = self.split_off_agg_column(df,'donor','name')

        df.datetime = pd.to_datetime(df.datetime)
        df.set_index('datetime', inplace=True)
        df = df.groupby('donor').resample('W-MON', label='left').sum().sum(axis=1).reset_index()
        df = df.set_index('donor').join(efficiency).join(names)
        df['value'] =  df[0] * df['efficiency'] / 100
        return df[['datetime','name','value']]

    def merge_donors(self, df):
        donors = pd.concat([pd.read_csv(self.ROOT_FOLDER + self.ngo + '/' + fx) for
            fx in self.donor_csvs()])
        cols = ['id','efficiency', 'name_en']
        donors = donors[cols]
        donors.columns = ['donor','efficiency', 'name']
        
        return df.merge(donors, on='donor')

    def split_off_agg_column(self,df,agg,col):
        split_col = df.groupby(agg).min()[col]
        df.drop(col, axis=1, inplace=True)
        return split_col

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
                left:0;
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
        cut = max(len(monthly_totals) - 1, 0)
        base_week = datetime(self.YEAR_NUM, len(monthly_totals), 1).isocalendar()[1]
        month_names = list(self.month_names[:len(monthly_totals)])
        month_styles = list(self.month_styles[:len(monthly_totals)])

        for offset, weekly_total in enumerate(weekly_totals):
            monthly_totals.insert(cut + offset, weekly_total)
            month_names.insert(cut + offset, 'WK' + str(base_week + offset))
            month_styles.insert(cut + offset, self.weekly_style)
        
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

    def compose_layout(self, x, y, weeks, opts):
        title = """Number of Meals Provided in {month}
            (Week {week_s} - Week {week_e}) {year} : {meals}""".format(**opts)
        layout = go.Layout(
            title='<b>{name}</b><br>{year} Weekly Food Donation in KG '.format(**opts),
            titlefont = dict(
                size=22
                ),
            xaxis=dict(
                # set x-axis' labels direction at 45 degree angle
                tickangle=-45,
                title=title,
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

    def relevant_csvs(self):
        return [fn for fn in self.available_csvs() if
            ".".join([str(self.YEAR_NUM), 'collection']) in fn]

    def donor_csvs(self):            
        return [fn for fn in self.available_csvs() if 'donor' in fn]
    
    def base_path(self):
        return self.ROOT_FOLDER + self.ngo + '/'

    def ensure_dest_exists(self):
        if not os.path.exists( self.path_pdf):
            os.makedirs( self.path_pdf)

    def clean_temp_files(self):
        raise NotImplementedError

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