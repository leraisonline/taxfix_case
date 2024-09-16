import logging
import sqlite3
from datetime import datetime

import matplotlib.pyplot as plt
import seaborn as sns
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import (
    Image,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

CONFIG = {"db_path": "persons.sqlite", "output_path": "reports/"}


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def generate_report(db_path: str, output_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
    WITH base_data AS (
        SELECT
            COUNT(*) OVER () as total_users,
            email_provider = 'gmail.com' as is_gmail,
            country,
            age_group,
            CAST(SUBSTR(age_group, 2, 2) AS INTEGER) as age_start
        FROM persons
    ),
    gmail_stats AS (
        SELECT
            MAX(total_users) as total_users,
            SUM(CASE WHEN country = 'Germany' AND is_gmail THEN 1 ELSE 0 END) as germany_gmail_users,
            SUM(CASE WHEN is_gmail AND age_start >= 60 THEN 1 ELSE 0 END) as over_60_gmail_count
        FROM base_data
    ),
    country_counts AS (
        SELECT country, COUNT(*) as count,
               RANK() OVER (ORDER BY COUNT(*) DESC) as rank
        FROM base_data
        WHERE is_gmail
        GROUP BY country
    ),
    age_distribution AS (
        SELECT age_group, COUNT(*) as count
        FROM base_data
        WHERE is_gmail
        GROUP BY age_group
    )
    SELECT
        gs.total_users,
        ROUND(100.0 * gs.germany_gmail_users / gs.total_users, 2) as germany_gmail_percentage,
        (
            SELECT GROUP_CONCAT(country || ':' || count, '; ')
            FROM (
                SELECT country, count
                FROM country_counts
                WHERE rank <= 3
                ORDER BY rank
            )
        ) as top_three_countries,
        gs.over_60_gmail_count,
        (
            SELECT GROUP_CONCAT(age_group || ':' || count, '; ')
            FROM age_distribution
            ORDER BY age_group
        ) as age_distribution
    FROM gmail_stats gs
    """

    cursor.execute(query)
    result = cursor.fetchone()
    conn.close()

    (
        total_users,
        germany_gmail_percentage,
        top_countries_str,
        over_60_gmail_count,
        age_distribution_str,
    ) = result

    # top three countries
    top_countries = [
        dict(zip(["country", "count"], country.split(":")))
        for country in top_countries_str.split("; ")
    ]
    for country in top_countries:
        country["count"] = int(country["count"])

    # age distribution
    age_distribution = [
        dict(zip(["age_group", "count"], age_group.split(":")))
        for age_group in age_distribution_str.split("; ")
    ]
    for age_group in age_distribution:
        age_group["count"] = int(age_group["count"])

    # visualizations
    plt.figure(figsize=(15, 5))

    # chart for Gmail usage in Germany
    plt.subplot(131)
    labels = "Gmail Users in Germany", "Other Users"
    sizes = [germany_gmail_percentage, 100 - germany_gmail_percentage]
    plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=90)
    plt.title("Percentage of users live in Germany and use Gmail")

    # chart for top countries using Gmail
    plt.subplot(132)
    countries = [item["country"] for item in top_countries]
    counts = [item["count"] for item in top_countries]
    sns.barplot(x=countries, y=counts)
    plt.title("Top 3 Countries Using Gmail")
    plt.ylabel("Number of Users")

    # Age distribution of Gmail users
    plt.subplot(133)
    age_groups = [item["age_group"] for item in age_distribution]
    age_counts = [item["count"] for item in age_distribution]
    sns.barplot(x=age_groups, y=age_counts)
    plt.title("Age Distribution of Gmail Users")
    plt.xticks(rotation=45)
    plt.ylabel("Number of Users")

    plt.tight_layout()

    # Generate PDF report
    file_name = datetime.utcnow().strftime(
        "%Y-%m-%d %H:%M:%S"
    )  # we use here for now() - but in production it will be dag start time
    output_path_file = output_path + file_name + ".pdf"
    doc = SimpleDocTemplate(output_path_file, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("Data Analysis Report", styles["Title"]))
    elements.append(Spacer(1, 12))

    summary = f"""
    Total Users: {total_users}<br/>
    Percentage of users live in Germany and use Gmail: {germany_gmail_percentage:.2f}%<br/>
    Number of people over 60 using Gmail: {over_60_gmail_count}
    """
    elements.append(Paragraph("Summary", styles["Heading2"]))
    elements.append(Paragraph(summary, styles["BodyText"]))
    elements.append(Spacer(1, 12))

    # top 3 Countries Table
    elements.append(Paragraph("Top 3 Countries Using Gmail", styles["Heading2"]))
    data = [["Rank", "Country", "Number of Users"]] + [
        [i + 1, item["country"], item["count"]] for i, item in enumerate(top_countries)
    ]
    t = Table(data)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 14),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("TEXTCOLOR", (0, 1), (-1, -1), colors.black),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, 0), 12),
                ("TOPPADDING", (0, 1), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ]
        )
    )
    elements.append(t)

    img_path = CONFIG["output_path"] + "/temp_plot.png"
    plt.savefig(img_path, format="png")
    elements.append(Spacer(1, 12))
    elements.append(Paragraph("Data Visualizations", styles["Heading2"]))
    elements.append(Image(img_path, width=500, height=200))

    # generate the PDF
    doc.build(elements)

    logging.info(f"Report generated and saved as {output_path_file}")


generate_report(**CONFIG)
