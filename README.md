# Simulating-OLTP-with-Apache-Airflow.-An-ETL-Pipeline-Project

**Introduction**

Let’s dive into it, this report gives you a rundown of a cool data engineering ETL (Extract, Transform, Load) pipeline that was put together using some awesome technologies. We used IBM Cloud DB2 as the relational database, Theia as the Integrated Development Environment (IDE), Linux as the operating system, and Python as the programming language. The pipeline is designed to simulate a simple scenario where we process data in near real-time, just like an online transaction processing (OLTP) system. Oh, and it runs every single minute!

**Relational Database: IBM Cloud DB2**

I decided to go with IBM Cloud DB2 as my trusty relational database management system (RDBMS), also because it is free. It's a solid choice for storing and managing data, offering features that keep our data secure, maintain its integrity, and optimize performance. Plus, it supports SQL-based operations, making it super convenient to work with.

**Integrated Development Environment (IDE): Theia**

For our coding needs, we picked Theia, a fantastic web-based IDE. It provides a slick environment where we can write and manage our ETL pipeline. The best part? Since it's web-based, we can access it from anywhere and collaborate easily. Theia comes packed with cool features like code editing, version control integration, and the ability to add extensions for extra functionality.

**Operating System: Linux**

To run my ETL pipeline, I chose Linux as my operating system. Linux offers a stable and customizable environment that's perfect for hosting my data engineering processes. I love how easy it is to manage shell scripts and automate tasks using CRONTAB its command-line interface.

**Programming Language: Python**

Python was my language of choice for implementing the ETL pipeline, it is one of my favorite languages, so that was a no brainier, and boy, did it make my life easier! Python is widely used in data engineering because of its simplicity, versatility, and amazing library ecosystem. With Python, I had all the tools needed for data manipulation, transformation, and connecting to databases like DB2.

**ETL Frequency: Every Minute**

I set my ETL pipeline to run every single minute. Yep, you heard that right! The  pipeline extracts, transforms, and loads data from a cdv file in a local folder in Theia to the DB2 database in a jiffy. This super-fast frequency ensures that our data is always up to date and reflects the latest changes.

**ETL Process: Simulating Simple OLTP**

My Python code simulates a simple OLTP scenario, mimicking how online transactions work. I grab a single row from a cdv file in a local folder in Theia, do some transformations to make it fit my desired data model, and then update the corresponding table in the DB2 database. It's a nifty way to keep things moving and ensure my database stays current.

**Conclusion**

To wrap it up, my data engineering ETL pipeline is a pretty cool solution. The combo of IBM Cloud DB2, Theia IDE, Linux, and Python gives us a reliable and efficient setup. Running the pipeline every minute keeps our data fresh and ready for analysis. This was a simple simulation of OLTP, and could be scaled up to handle higher influx of data using the same pipeline setup. I am excited about the possibilities this pipeline opens up, like scaling our infrastructure, giving real-time data analysis and visualizations, adding error handling, and expanding our transformations to handle even more complex scenarios.


The sky's the limit when it comes to enhancing this pipeline! I can integrate more data sources, dive into advanced analytics, and adapt to changing business requirements. With this setup, I am confident that I’ve got a solid foundation for efficient data processing, giving us up-to-date information to make smarter decisions and conduct meaningful analysis.
