#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function)

import calendar
import json
import os
import pandas as pd
import plotly.graph_objs as go
import shutil

from datetime import datetime, timedelta
from math import ceil
from plotly import tools, utils
from subprocess import call


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
        
        self.ngo = ngo
        self.meal_weight = 0.42
        self.pickup_type = 'food'

        # Dates

        self.PERIOD = (datetime.now() - timedelta(days=28))
        self.MONTH_NUM = self.PERIOD.month
        self.MONTH_NAME = self.PERIOD.strftime('%B')
        self.YEAR_NUM = self.PERIOD.year

        # self.MONTH_NUM = 4
        # self.MONTH_NAME = 'April'
        # self.MONTH_NUM = 5
        # self.MONTH_NAME = 'May'
        # self.MONTH_NUM = 6
        # self.MONTH_NAME = 'June'

        self.month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        self.month_styles = ['rgba(204,204,204,1)']
        self.weekly_style = ['rgba(222,45,38,0.8)']

        self.start_date = datetime(self.YEAR_NUM, self.MONTH_NUM, 1)
        self.end_date = datetime(self.YEAR_NUM, self.MONTH_NUM, calendar.monthrange(
            self.YEAR_NUM, self.MONTH_NUM)[1])

        # Filenames
        
        self.ROOT_FOLDER = 'data/Canonical/'
        self.REPORT_FOLDER = 'data/Reports/Donors/'
        
        self.fn = ''
        self.fn_html = self.fn + '.html'
        self.fn_pdf = self.fn + '.pdf'
        self.path_pdf = self.REPORT_FOLDER + ngo + " Donor Reports" + '/' + str(self.YEAR_NUM) + '/' + \
            str(self.MONTH_NUM) + '/'

        # System Calls

        self.html_to_pdf_process = [
            "wkhtmltopdf", "--debug-javascript", "--orientation", "Landscape",
            "-T", "5", "-B", "5", "-L", "5", "-R", "5", self.fn_html,
            self.path_pdf + self.fn_pdf ] 


    # PUBLIC

    def data_to_pdf(self):

        df_w, df_m = self.prepare_data()

        for donor in df_w.index.unique():
            self.set_fns(donor)

            print("\n>>>> {}".format(donor.upper()))

            w_totals = df_w.loc[[donor],'value'].fillna(0).astype(int).tolist()
            try:
                w_labels = ['WK'+str(wk) for wk in df_w.loc[donor,'datetime'].dt.week.tolist()]
            except AttributeError:
                w_totals = df_w.loc[donor,['value']].astype(int).values.tolist()
                w_labels = ['WK'+str(wk) for wk in df_w.loc[donor,['datetime']].dt.week.tolist()]
            
            # print(w_totals)
            # print(w_labels)

            m_totals = df_m.loc[[donor],'value'].fillna(0).astype(int).tolist()
            try:
                m_labels = df_m.loc[donor,'datetime'].dt.strftime('%b').str.upper().tolist()
            except AttributeError:
                m_totals = df_m.loc[donor,['value']].astype(int).values.tolist()
                m_labels = df_m.loc[donor,['datetime']].dt.strftime('%b').str.upper().tolist()

            # print(m_totals)
            # print(m_labels)

            data, x, y = self.compose_data(w_totals, w_labels ,m_totals, m_labels)
            # print(data)
            print("{}{}".format('LABELS:', x))
            print("{}{}\n".format('VALUES:', y))

            m_total = sum(w_totals)
            w_count = len(w_totals)
            opts = self.opts(df_w, donor, m_total)
            
            layout = self.compose_layout(x, y, w_count, opts)
            
            fig = go.Figure(data=data, layout=layout)
            html = self.new_iplot(fig)
            
            self.html_to_pdf(html)

        self.clean_temp_files()

    def opts(self, df, donor, monthly_total):
        try:
            name = df.loc[donor,'name'].values[0]
        except AttributeError:
            name = df.loc[donor,'name']
        return dict(
            name = name,
            month = self.start_date.strftime('%b'),
            week_s = self.start_date.isocalendar()[1],
            week_e = self.end_date.isocalendar()[1],
            year = self.YEAR_NUM,
            meals = int(monthly_total / self.meal_weight)
        )

    def set_fns(self,donor):
        self.fn = "{}.{}.{}.{}.{}.report".format(self.ngo, donor, self.YEAR_NUM, self.MONTH_NUM, self.pickup_type)
        self.fn_html = self.fn + '.html'
        self.fn_pdf = self.fn + '.pdf'
        self.html_to_pdf_process = [
            "wkhtmltopdf", "--debug-javascript", "--orientation", "Landscape",
            "-T", "5", "-B", "5", "-L", "5", "-R", "5", 'temp/' + self.fn_html,
            self.path_pdf + self.fn_pdf ] 

    # PRIVATE

    # Data Wrangling

    def prepare_data(self):

        df = pd.concat([pd.read_csv(self.ROOT_FOLDER + self.ngo + '/' + fx, encoding='utf8') for
            fx in self.relevant_csvs()]).fillna(0)

        df.donor = df.donor.str.lower()

        df = self.merge_donors(df)

        df.datetime = pd.to_datetime(df.datetime)
        
        efficiency = self.split_off_agg_column(df,'donor','efficiency')
        names = self.split_off_agg_column(df,'donor','name')
        
        df = df.set_index('datetime')

        df = df[df.index < self.end_date]

        df_m = df.groupby('donor').resample('M', label='right').sum().sum(1).reset_index()

        df_m = df_m.set_index('donor').join(efficiency).join(names)
        if df_m['efficiency'].isnull().any():
            raise ValueError(", ".join(df_m[df_m['efficiency'].isnull()].name.values) + " don't have their efficiency set.")
        df_m['value'] =  df_m[0] * self.get_efficiency(df_m) / 100

        df_w = df.groupby('donor').resample('W-MON', label='left').sum().sum(1).reset_index()
        df_w = df_w.set_index('donor').join(efficiency).join(names)
        df_w['value'] =  df_w[0] * self.get_efficiency(df_w) / 100
        df_w = self.slice_reporting_month(df_w)
        
        return df_w[['datetime','name','value']], df_m[['datetime','name','value']]

    def get_efficiency(self, df_):
        return df_['efficiency']

    def merge_donors(self, df):
        donors = pd.concat([pd.read_csv(self.ROOT_FOLDER + self.ngo + '/' + fx, encoding='utf8') for
            fx in self.donor_csvs()])
        cols = ['id','efficiency', 'name_en']
        donors = donors[cols]
        donors.name_en = donors.name_en.str.strip()
        donors.id = donors.id.str.strip().str.lower()

        donors.columns = ['donor','efficiency', 'name']
        donors = donors.drop_duplicates()
        donors = donors.sort_values('name').reset_index(drop=True)

        return df.merge(donors, on='donor', how='left')

    def split_off_agg_column(self,df,agg,col):
        split_col = df.groupby(agg).min()[col]
        df.drop(col, axis=1, inplace=True)
        return split_col

    # Plotting

    def new_iplot(self, figure_or_data, validate=True):

        figure = tools.return_figure_from_figure_or_data(figure_or_data, validate)
            
        jdata = json.dumps(figure.get('data', []), cls=utils.PlotlyJSONEncoder)
        jlayout = json.dumps(figure.get('layout', {}), cls=utils.PlotlyJSONEncoder)

        config = {}
        config['showLink'] = False
        config['linkText'] = None
        
        jconfig = json.dumps(config)

        html = ''
        
        script = 'Plotly.plot(gd, {data}, {layout}, {config})'.format(
                data=jdata,
                layout=jlayout,
                config=jconfig)

        with open('assets/plotly-latest.min.js') as js:
            plotly = js.read()

        with open('scripts/report/foodlink/report.template.html') as template:
            html += template.read().format(id='placeholder', script=script, plotly=plotly)

        return html
    
    # Data

    def compose_data(self, w_totals, w_labels, p_totals, p_labels):
        p_styles = self.month_styles * len(p_totals)
        p_styles[-1:-1] = self.weekly_style * len(w_totals)
        p_totals[-1:-1] = w_totals
        p_labels[-1:-1] = w_labels
        
        trace0 = go.Bar(
            x=p_labels,
            y=p_totals,
            marker=dict(
                color=p_styles
            )
        )
        data = [trace0]
        return data, p_labels, p_totals

    # Layout

    def compose_layout(self, x, y, weeks, opts):
        x_title = """<b>Equivalent of {meals} meals provided in {month} {year}</b><br>(Week {week_s} - Week {week_e})""".format(**opts)
        layout = go.Layout(
            title=u'<b>{name}</b><br>{year} Weekly Food Donation in KG '.format(**opts),
            titlefont = dict(
                size=28
                ),
            xaxis=dict(
                # set x-axis' labels direction at 45 degree angle
                # tickangle=-45,
                title=x_title,
                tickfont=dict(
                    size=20
                ),
                titlefont = dict(
                    size=24
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

    def html_to_pdf(self, html):

        self.ensure_dest_exists()

        with open('temp/' + self.fn_html, 'w') as fn:
            fn.write(html)

        call(self.html_to_pdf_process)

    # Utilities 

    def available_csvs(self):
        return os.listdir(self.base_path())

    def relevant_csvs(self):
        return [fn for fn in self.available_csvs() if
            ".".join([str(self.YEAR_NUM), 'collection']) in fn and 'Amenities' not in fn]

    def donor_csvs(self):            
        return [fn for fn in self.available_csvs() if 'donor' in fn]
    
    def base_path(self):
        return self.ROOT_FOLDER + self.ngo + '/'

    def ensure_dest_exists(self):
        for path in ['temp',self.path_pdf]:
            if not os.path.exists(path):
                os.makedirs(path)

    def clean_temp_files(self):
        self.ensure_dest_exists()
        shutil.rmtree('temp')


    def week_of_month(self, dt):
        """ Returns the week of the month for the specified date.
        """

        first_day = dt.replace(day=1)

        dom = dt.day
        adjusted_dom = dom + first_day.weekday()

        return int(ceil(adjusted_dom/7.0))

    def slice_reporting_month(self, df):
        mask = (df.datetime > self.start_date) & (df.datetime <= self.end_date)
        return df[mask]


if __name__ == '__main__':
    opts = {
        "ngo" : 'FoodLink',
    }

    report = FoodLinkDonorReport(**opts)
    report.data_to_pdf()
