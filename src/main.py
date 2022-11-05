from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import year as pyspark_year
from pyspark.sql.types import *
from pyspark.sql.window import *
from operator import itemgetter
import sys


class Data:

    def __init__(self, aircrafts_file, airlines_file, airports_file,
                 flights_file):
        self._aircrafts_file = aircrafts_file
        self._airlines_file = airlines_file
        self._airports_file = airports_file
        self._flights_file = flights_file
        self._spark = SparkSession.builder.appName("data").getOrCreate()

    def aircrafts_df(self):
        df = self._spark.read.csv(self._aircrafts_file,
                                  inferSchema=True,
                                  header=True)
        return df

    def airlines_df(self):
        df = self._spark.read.csv(self._airlines_file,
                                  inferSchema=True,
                                  header=True)
        return df

    def airports_df(self):
        df = self._spark.read.csv(self._airports_file,
                                  inferSchema=True,
                                  header=True)
        return df

    def flights_df(self):
        df = self._spark.read.csv(self._flights_file,
                                  inferSchema=True,
                                  header=True)
        return df


class Top_three:

    def __init__(self, data):
        self.spark = SparkSession.builder.appName("top_3").getOrCreate()
        self._data = data

    def _perform_analysis(self):
        # Filter nulls to reduce the size of the dataset
        aircrafts = self._data.aircrafts_df().filter((col("model").isNotNull())
                                                     & (~isnan(col("model"))))
        aircrafts.cache()

        # Filter CESSNA models
        aircrafts = aircrafts.filter(
            col("tailnum").isNotNull() & aircrafts["model"] == "CESSNA")

        # Regex to extract first 3 digits from model name, and discard rest of columns.
        #  Select is slightly better than withColumn
        aircrafts = aircrafts.select(
            "tailnum",
            regexp_extract(aircrafts["model"], '\d{3}', 0).alias("model_name"))

        # Clean flights
        #   Undefined how we should deal with flights that have missing sch/arrival times
        flights = self._data.flights_df().filter(
            col("tail_number").isNotNull()).select("tail_number")
        flights.cache()

        flights = flights.groupBy("tail_number").count()

        # Join flights with cessna aircrafts
        # Right join because we only want cessna flights
        flights = flights.join(broadcast(aircrafts),
                               flights.tail_number == aircrafts.tailnum,
                               "right")

        # Group by model and sum the counts since each model can have multiple tail numbers
        flights = flights.groupBy("model_name").agg(
            sum(flights.count).alias("num_flights"))

        # Sort by flights and get top 3
        flights = flights.orderBy(col("num_flights").desc()).limit(3)

        # Return list of Row objects since that's what we're going to use
        #  Better to return the list via collect() here since flights is already cached
        return flights.collect()

    def format_output(self, row_list):
        for o in row_list:
            yield f"CESSNA {o[0]} \t {o[1]}"

    def print_output(self):
        row_list = self._perform_analysis()
        for i in self.format_output(row_list):
            print(i)


class Avg_delay:

    def __init__(self, provided_year, data):
        self.spark = SparkSession.builder.appName("avg_delay").getOrCreate()
        self._data = data
        self._provided_year = provided_year

    def _perform_analysis(self):
        flights = self._data.flights_df()
        flights.cache()

        # Clean flights df
        flights = flights.filter((col("scheduled_depature_time").isNotNull())
                                 & (~isnan(col("scheduled_depature_time"))))

        flights = flights.filter((col("actual_departure_time").isNotNull())
                                 & (~isnan(col("actual_departure_time"))))

        flights = flights.withColumn(
            "flight_year", pyspark_year(col("flight_date").cast(DateType())))

        # Filter flights by year
        flights = flights.filter(flights["flight_year"] == self._provided_year)

        # Clean carrier code for future cases
        flights = flights.filter((col("carrier_code").isNotNull()))

        # Convert depature times to timestamp
        flights = flights.select(
            "carrier_code",
            to_timestamp(
                when(flights["scheduled_depature_time"] == "24:00:00",
                     "00:00:00").otherwise("HH:mm:ss")).alias(
                         "scheduled_departure_time"),
            to_timestamp(
                when(flights["actual_departure_time"] == "24:00:00",
                     "00:00:00").otherwise("HH:mm:ss")).alias(
                         "actual_departure_time"))

        # Calculate delay:
        #   - if delay between 0 - 12: return delay as is
        #   - if delay < -12: assume flight was delayed by a day
        #   - if -12 < delay < 0: means flight was early
        flights = flights.select(
            "carrier_code",
            ((flights["actual_departure_time"].cast("long") -
              flights["scheduled_departure_time"].cast("long")) /
             60).alias("delay_mins"))

        # First, discard flights with no delays - reduces dataset
        flights = flights.filter(col("delay_mins") != 0)

        # Also rename carrier_code so it doesnt conflict upon join
        flights = flights.select(
            col("carrier_code").alias("fl_carrier_code"),
            when(col("delay_mins") / 60 <= 12, col("delay_mins")).when(
                col("delay_mins") / 60 < -12,
                ((24 * 60) +
                 col("delay_mins"))).otherwise(-2).alias("final_delay"))

        # Fliter flights that were delayed
        flights = flights.filter(col("final_delay") > 0)

        # Clean airlines df
        airlines = self._data.airlines_df().filter(
            col("carrier_code").isNotNull() & col("name").isNotNull())
        airlines.cache()

        flights = flights.join(
            broadcast(airlines),
            airlines.carrier_code == flights.fl_carrier_code, "left")

        flights = flights.groupBy("name").agg(
            count(flights.final_delay).alias("num_delays"),
            round(mean(flights.final_delay)).alias("avg_delay"))

        return flights.collect()

    def format_output(self, row_list):
        for o in row_list:
            yield f"{o[0]} \t {o[1]} \t {o[2]}"

    def print_output(self):
        row_list = self._perform_analysis()
        row_list.sort(key=itemgetter(0))
        for i in self.format_output(row_list):
            print(i)


class Top_aircrafts:

    def __init__(self, data):
        self.spark = SparkSession.builder.appName("avg_delay").getOrCreate()
        self._data = data

    def _perform_analysis(self):
        flights = self._data.flights_df()
        flights.cache()

        # Clean flights
        flights = flights.filter(
            col("actual_departure_time").isNotNull()
            & col("tail_number").isNotNull())

        # Count number of flights per tail number
        flights = flights.groupBy("tail_number", "carrier_code").count()

        aircrafts = self._data.aircrafts_df().select("tailnum", "manufacturer",
                                                     "model")
        aircrafts.cache()

        # Clean aircrafts
        aircrafts = aircrafts.filter(
            col("manufacturer").isNotNull() & col("model").isNotNull())

        # Create new name columns as per specification
        aircrafts = aircrafts.select(
            "tailnum",
            concat(col("manufactorer"), " ", col("model")).alias("manu_model"))

        airlines = self._data.airlines_df()
        airlines.cache()

        # Clean airlines
        airlines = airlines.filter(
            col("carrier_code").isNotNull() & col("name").isNotNull())

        # Change airlines carrier_code column name so it's not the same as flights'
        airlines = airlines.select(
            col("carrier_code").alias("al_carrier_code"), "name")

        # Join flights with airlines
        #   Name refers to the airline's name
        flights = flights.join(
            broadcast(airlines),
            flights.carrier_code == airlines.al_carrier_code,
            "left").select("tail_number", "count", "name")

        # Join flights with aircrafts
        flights = flights.join(broadcast(aircrafts),
                               flights.tail_number == aircrafts.tailnum,
                               "left")

        # Sum the number of flights by airline name AND model
        flights = flights.groupBy("name", "manu_model").agg(
            sum("count").alias("num_flights"))

        flights = flights.filter(col("manu_model").isNotNull())

        model_window = Window.partitionBy("name").orderBy(
            col("num_flights").desc())

        # Rank each model of an airline by their flights and get the top 5
        flights = flights.select(
            "name", "manu_model", "num_flights",
            rank().over(model_window).alias("rank")).filter(col("rank") <= 5)

        # Put the top 5 models in a list, per airline
        flights = flights.groupBy("name").agg(
            collect_list("manu_model").alias("top_5_models")).select(
                "name", "top_5_models")

        # Now each airline should have a corressponding list of top 5 models
        return flights.collect()

    def format_output(self, row_list):
        for o in row_list:
            yield f"{o[0]} \t [{', '.join(o[1])}]"

    def print_output(self):
        row_list = self._perform_analysis()
        row_list.sort(key=itemgetter(0))
        for i in self.format_output(row_list):
            print(i)


def main():
    analysis_type = sys.argv[1]  # User specifies the analysis they wish to run

    # aircrafts file
    ac = "ontimeperformance_aircrafts.csv"

    # airplines file
    al = "ontimeperformance_airlines.csv"

    # airports file
    ap = "ontimeperformance_airports.csv"

    # flights file
    fl = sys.argv[2]  # "ontimeperformance_flights_tiny.csv"

    # init data object
    data = Data(ac, al, ap, fl)

    if analysis_type == "a":
        analysis = Top_three(data)
        analysis.print_output()
    elif analysis_type == "b":
        provided_year = int(sys.argv[3]) if sys.argv[3] is not None else 1994
        analysis = Avg_delay(data=data, provided_year=provided_year)
        analysis.print_output()
    elif analysis_type == "c":
        analysis = Top_aircrafts(data=data)
        analysis.print_output()
    else:
        print("Usage ...")


if __name__ == "__main__":
    main()
