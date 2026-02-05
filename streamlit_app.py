import warnings
import random
import polars as pl
import s3fs
import streamlit as st
from dotenv import load_dotenv
import polars.selectors as cs
import math
import altair as alt
import pandas as pd


AWS_ACCESS_KEY_ID = "tid_xpRCdsbJTvJoNOJzygerAAVUDPlKVvGwGOCLwyuPvsZUnBnrEC"
AWS_SECRET_ACCESS_KEY = (
    "tsec_lxcWdBpInYBlBfNw+kz7ySYDm+u+AsJW3eVGypI7Q3BUeyX8RfPPg2vFHO5jzI89XrvXJb"
)
AWS_ENDPOINT_URL_S3 = "https://t3.storage.dev"
# AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
AWS_ENDPOINT_URL_IAM = "https://iam.storage.dev"
AWS_REGION = "auto"
BUCKET_NAME = "damp-shape-1501"

s3 = s3fs.S3FileSystem(
    endpoint_url=AWS_ENDPOINT_URL_S3,
    key=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY,
)
warnings.filterwarnings("ignore")

load_dotenv()

aggs: list[pl.Expr] = [
    pl.col("additions").mean(),
    pl.col("changed_files").mean(),
    pl.col("deletions").mean(),
    pl.col("pull_count").mean(),
    pl.col("merged_duration").mean(),
    pl.col("closed_duration").mean(),
    pl.col("pull_duration").mean(),
    pl.col("closed").mean(),
    pl.col("merged").mean(),
    # pl.col("mergeable").mean()
    pl.col("commit_count").mean(),
    # pl.col("number").alias("pull_numbers")
]


@st.cache_data
def get_teams_data(
    org: str | None = "tolli-inc",
    team: str | None = None,
) -> pl.DataFrame:
    if team is None:
        with s3.open(f"{BUCKET_NAME}/{org}/team_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    with s3.open(f"{BUCKET_NAME}/{org}/team/{team}team_frame.ipc", "rb") as f:
        return pl.read_ipc_stream(f)


@st.cache_data
def get_contributors_data(
    org: str | None = "tolli-inc",
    team: str | None = None,
) -> pl.DataFrame:
    if team is None:
        with s3.open(f"{BUCKET_NAME}/{org}/contributor_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    with s3.open(f"{BUCKET_NAME}/{org}/team/{team}/contributor_frame.ipc", "rb") as f:
        return pl.read_ipc_stream(f)

@st.cache_data
def get_label(
    org: str | None = "tolli-inc",
    team: str | None = None,
    contributor: str | None = None
) -> pl.DataFrame:
    if team is None:
        with s3.open(f"{BUCKET_NAME}/{org}/contributor_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    with s3.open(f"{BUCKET_NAME}/{org}/team/{team}/contributor_frame.ipc", "rb") as f:
        return pl.read_ipc_stream(f)

@st.cache_data
def get_labels_count_data(
    org: str | None = "tolli-inc",
    team: str | None = None,
) -> pl.DataFrame:
    if team is None:
        with s3.open(f"{BUCKET_NAME}/{org}/label_count_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    with s3.open(f"{BUCKET_NAME}/{org}/team/{team}/label_count_frame.ipc", "rb") as f:
        return pl.read_ipc_stream(f)


@st.cache_data
def get_trends_data(
    org: str = "tolli-inc",
    team: str | None = None,
    contributor: str | None = None,
) -> pl.DataFrame:
    if contributor is None and team is None:
        with s3.open(f"{BUCKET_NAME}/{org}/trend_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    elif team is None:
        with s3.open(f"{BUCKET_NAME}/{org}/contributor/{contributor}/trend_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    elif contributor is None:
        with s3.open(f"{BUCKET_NAME}/{org}/team/{team}/trend_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    raise Exception("invalid code point")


@st.cache_data
def get_variances_data(
    org: str = "tolli-inc",
    team: str | None = None,
    contributor: str | None = None,
) -> pl.DataFrame:
    if contributor is None and team is None:
        with s3.open(f"{BUCKET_NAME}/{org}/variance_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    elif team is None:
        with s3.open(
            f"{BUCKET_NAME}/{org}/contributor/{contributor}/variance_frame.ipc", "rb"
        ) as f:
            return pl.read_ipc_stream(f)
    elif contributor is None:
        with s3.open(f"{BUCKET_NAME}/{org}/team/{team}/variance_frame.ipc", "rb") as f:
            return pl.read_ipc_stream(f)
    raise Exception("invalid code point")


st.markdown(
    """
<style>
    /* Target the specific div that wraps the image and remove border-radius */
    .st-emotion-cache-1dp5vir { /* This class name is specific to Streamlit's internal structure and might change with updates */
        border-radius: 0px !important;
    }
    /* A more generic approach that may work across versions: */
    /* Target the image element within the main block container */
    .block-container img {
        border-radius: 0px !important;
    }
</style>
""",
    unsafe_allow_html=True,
)

st.set_page_config(page_title="Tolli.ai", page_icon="favicon.svg", layout="wide")

col_buffer_left, col_content, col_buffer_right = st.columns([1, 10, 1])

org_login = "tolli-inc"
org_label = 3

with col_content:
    st.image("logoLight.svg")
    st.title("Demo (v.0.0.11)")
    st.text(
        "This is an industry overview as processed from various organizations.  All information is public.  This model is still in development and result should not be taken as fact."
    )
    # debug_info = st.toggle("Include supporting text.")

    with st.container(horizontal=True):
        org_label_count = get_labels_count_data()
        max_count = get_labels_count_data()["len"].max()
        polar_bars = alt.Chart(org_label_count).mark_arc(stroke='white', tooltip=True).encode(
            theta=alt.Theta("label", type="nominal"),
            radius=alt.Radius('len').scale(type='linear'),
            radius2=alt.datum(max_count * 0.08),
            color=alt.Color(field="label", type="nominal", legend=None),
        )
        # Create the circular axis lines for the number of observations
        # axis_rings = alt.Chart(pd.DataFrame({"ring": range(int(max_count * 0.08), max_count, 40)})).mark_arc(stroke='lightgrey', fill=None).encode(
        #     theta=alt.value(2 * math.pi),
        #     radius=alt.Radius('ring').stack(False)
        # )

        layer = alt.layer(
            # axis_rings,
            polar_bars,
            # title=['Current classification of contributors', '']
        )
        st.altair_chart(layer)

    with st.container():
        with st.expander("Debug Info"):
            trend_tab, variance_tab, cluster_tab = st.tabs(
                ["Trend", "Variance", "Clustering"]
            )
            with trend_tab:
                # if debug_info:
                #     st.info('A trend in a graph is the general, long-term direction or tendency of data points over time, indicating whether values are increasing, decreasing, or staying stable.', icon="ℹ️")
                st.altair_chart(
                    alt.Chart(
                        get_trends_data()
                        .unpivot(
                            cs.numeric(),
                            index=["org_login", "date"]
                        )
                    )
                    .mark_line().encode(x="date:T", y="value:Q", color="variable:N")
                )
            with variance_tab:
                # if debug_info:
                #     st.info('Variance measures the spread of a data distribution relative to its mean, helping compare variability across datasets with different scales', icon="ℹ️")
                st.altair_chart(
                    alt.Chart(
                        get_variances_data()
                        .unpivot(
                            cs.numeric(),
                            index=["org_login", "date"]
                        )
                    )
                    .mark_line().encode(x="date:T", y="value:Q", color="variable:N")
                )

            with cluster_tab:
                polar_bars = alt.Chart(get_labels_count_data()).mark_arc(stroke='white', tooltip=True).encode(
                    theta=alt.Theta("label", type="nominal"),
                    radius=alt.Radius('len').scale(type='linear'),
                    radius2=alt.datum(1),
                    color=alt.Color(field="label", type="nominal", legend=None),
                )
                layer = alt.layer(
                    polar_bars,
                    # title=['Current classification of contributors', '']
                )
                st.altair_chart(layer)

    with st.container():
        st.header("Clustering")
        st.text(
            "This is where we show the attributes of each team and engineer.  The tabs give some debug insight."
        )

        _teams = {_team["slug"]: _team for _team in get_teams_data().rows(named=True)}

        teams = st.multiselect(
            label="Choose teams",
            options=list(_teams.keys()),
            default=["microsoft", "facebook", "flutter"],
            format_func=lambda x: _teams[x]["name"],
        )
        if not teams:
            st.error("Please select at least one team.")
        else:
            for team in teams:
                with st.container():
                    with st.container(horizontal=True):
                        st.image(_teams[team]["avatar_url"], width=68)
                        st.space("xxsmall")
                        st.subheader(_teams[team]["name"])
                        st.space("xxsmall")
                        st.badge(_teams[team]["label"], help="The current classification of the team.")
                    with st.container(horizontal=True):
                        polar_bars = alt.Chart(get_labels_count_data(team=team)).mark_arc(stroke='white', tooltip=True).encode(
                            theta=alt.Theta("label", type="nominal"),
                            radius=alt.Radius('len').scale(type='linear'),
                            radius2=alt.datum(1),
                            color=alt.Color(field="label", type="nominal", legend=None),
                        )
                        layer = alt.layer(
                            polar_bars,
                            # title=['Current classification of contributors', '']
                        )
                        st.altair_chart(layer)


                    with st.expander("Contributors"):
                        for contributor in get_contributors_data(team=team).rows(named=True):
                            with st.container(horizontal=True):
                                st.space("xsmall")
                                st.image(contributor["avatar_url"], width=42)
                                st.space("xxsmall")
                                st.text(
                                    contributor["display_name"] or contributor["login"]
                                )
                                st.space("xxsmall")
                                st.badge(contributor["label"], help="The current classification of the user.")

                                
                    with st.expander("Debug Info"):
                        trend_tab, variance_tab, cluster_tab = st.tabs(
                            ["Trend", "Variance", "Clustering"]
                        )
                        with trend_tab:
                            st.altair_chart(
                                alt.Chart(
                                    get_trends_data(team=team)
                                    .unpivot(
                                        cs.numeric(),
                                        index=["org_login", "team_slug", "date"]
                                    )
                                ).mark_line().encode(x="date:T", y="value:Q", color="variable:N")
                            )
                            
                        with variance_tab:
                            st.altair_chart(
                                alt.Chart(
                                    get_variances_data(team=team)
                                    .unpivot(
                                        cs.numeric(),
                                        index=["org_login", "team_slug", "date"]
                                    )
                                ).mark_line().encode(x="date:T", y="value:Q", color="variable:N")
                            )
                        with cluster_tab:
                            polar_bars = alt.Chart(get_labels_count_data(team=team)).mark_arc(stroke='white', tooltip=True).encode(
                                theta=alt.Theta("label", type="nominal"),
                                radius=alt.Radius('len').scale(type='linear'),
                                radius2=alt.datum(1),
                                color=alt.Color(field="label", type="nominal", legend=None),
                            )
                            layer = alt.layer(
                                polar_bars,
                                # title=['Current classification of contributors', '']
                            )
                            st.altair_chart(layer)

            # for contributor in (
            #     get_contributors_data()
            #     .filter(pl.col("current_team_slug") == team)
            #     .rows(named=True)
            # ):
            #     buf, col1, col2 = st.columns([1, 1, 10])
            #     with col1:
            #         st.image(contributor["avatar_url"], width=68)
            #     with col2:
            #         st.subheader(contributor["display_name"])

        # data: Unknown = df.loc[countries]
        # data /= 1000000.0
        # st.subheader("Gross agricultural production ($B)")
        # st.dataframe(data.sort_index())

        # data = data.T.reset_index()
        # data = pd.melt(data, id_vars=["index"]).rename(
        #     columns={"index": "year", "value": "Gross Agricultural Product ($B)"}
        # )
        # chart = (
        #     alt.Chart(data)
        #     .mark_area(opacity=0.3)
        #     .encode(
        #         x="year:T",
        #         y=alt.Y("Gross Agricultural Product ($B):Q", stack=None),
        #         color="Region:N",
        #     )
        # )
        # st.altair_chart(chart, use_container_width=True)
    with st.container():
        st.header("Team Construction")
        st.text(
            "This is where we show the attributes of each team and engineer.  The tabs give some debug insight."
        )

        _contributors = {
            _contributor["login"]: _contributor
            for _contributor in get_contributors_data().rows(named=True)
        }
        contributors = st.multiselect(
            label="Choose contributors",
            options=list(_contributors.keys()),
            default=random.sample(list(_contributors.keys()), 7),
            format_func=lambda x: (
                _contributors[x]["display_name"] or _contributors[x]["login"]
            ),
        )
        if not contributors:
            st.error("Please select at least one contributors.")
        else:
            pass
