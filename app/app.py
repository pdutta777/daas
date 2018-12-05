#!/usr/bin/python

import snowflake.connector
from flask import Flask
from flask_restplus import Api, Resource, fields

app = Flask(__name__)
api = Api(app, version='1.0', title='Billing CSV Api', description='An Api to Retrieve Billing Data For a Customer')

input_model = api.model("input", {
    "pid": fields.String("Customer ID"),
    "start_date": fields.String("Start date"),
    "end_date": fields.String("End date")
})
billing_model = api.model("billing_calls", {
    "day": fields.String("Billing Date"),
    "type": fields.String("Billing Type"),
    "count": fields.String("Count")
})


@api.route('/billing/calls/')
class BillingCalls(Resource):

    @api.marshal_with(billing_model, envelope='data')
    @api.expect(input_model)
    def post(self):
        """
        get all the billing data
        """

        inp = api.payload
        # Get the password secret
        fp = open('/tmp/sfpass/secrets.txt')
        line = fp.readline()
        fp.close()
        pw = line.rstrip()
        conn = snowflake.connector.connect(
            user='aam_tableau',
            password=pw,
            account='adobe',
            database='aam_prod_datawarehouse',
            schema='dw',
            role='aam_role_readonly',
            warehouse='dev_pdutta',
            insecure_mode=True
        )
        curs = conn.cursor()
        curs.execute(
            "select day::string as day,case when traittype=3 AND d_event=1 THEN 'PIXEL' "
            "WHEN traittype=3 AND d_event=0 THEN 'ONSITE' "
            "WHEN traittype=10 THEN 'ALL-ONBOARDED' "
            "END AS TYPE, SUM(count) AS COUNT "
            "FROM report_billing_events "
            "WHERE pid={0} "
            "AND day >= '{1}' AND day <= '{2}' "
            "AND (server_call_type <> 'SignalsCenter' "
            "OR server_call_type IS NULL) "
            "GROUP BY 1,2 ORDER BY 1,2".
            format(inp['pid'], inp['start_date'], inp['end_date']))

        # Get a list of column names
        column_names = list(map(lambda x: x.lower(), [d[0] for d in curs.description]))
        rows = list(curs.fetchall())
        result = [dict(zip(column_names, row)) for row in rows]
        curs.close()
        conn.close()
        return result, 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
