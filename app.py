import streamlit as st
import pandas as pd
from datetime import date
import asyncio
import aiohttp
import time
from dateutil import parser
import altair as alt
import nest_asyncio

# allow asyncio to work with Streamlit
nest_asyncio.apply()

# -----------------------------
# API Fetch Functions
# -----------------------------

async def fetch_project_tasks(session, project_id):
    url = f"https://tasking-manager-production-api.hotosm.org/api/v2/projects/{project_id}/tasks/"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        st.error(f"Error fetching project tasks: {e}")
        return None

async def fetch_task_details(session, project_id, task_id):
    url = f"https://tasking-manager-production-api.hotosm.org/api/v2/projects/{project_id}/tasks/{task_id}/"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except Exception:
        return None

# -----------------------------
# Date Parser
# -----------------------------

def parse_datetime(date_string):
    if not date_string:
        return None
    try:
        return parser.isoparse(date_string.rstrip('Z'))
    except Exception:
        return None

# -----------------------------
# Compute Mapping & Validation Summary
# -----------------------------

def compute_mapval_summary(df: pd.DataFrame) -> pd.DataFrame:
    # --- Mapped ---
    mapped = (
        df[df["action"].isin(["LOCKED_FOR_MAPPING", "STATE_CHANGE"])]
        .groupby(["taskId", "actionBy"])["action"]
        .apply(set)
        .reset_index()
    )
    mapped = mapped[mapped["action"].apply(lambda acts: {"LOCKED_FOR_MAPPING", "STATE_CHANGE"}.issubset(acts))]
    mapped_summary = mapped.groupby("actionBy")["taskId"].nunique().reset_index()
    mapped_summary.columns = ["user", "mapped_tasks"]

    # --- Validated ---
    validated = (
        df[df["action"].isin(["LOCKED_FOR_VALIDATION", "STATE_CHANGE"])]
        .groupby(["taskId", "actionBy"])["action"]
        .apply(set)
        .reset_index()
    )
    validated = validated[validated["action"].apply(lambda acts: {"LOCKED_FOR_VALIDATION", "STATE_CHANGE"}.issubset(acts))]
    validated_summary = validated.groupby("actionBy")["taskId"].nunique().reset_index()
    validated_summary.columns = ["user", "validated_tasks"]

    # --- Merge ---
    summary = pd.merge(mapped_summary, validated_summary, on="user", how="outer").fillna(0)
    summary[["mapped_tasks", "validated_tasks"]] = summary[["mapped_tasks", "validated_tasks"]].astype(int)

    return summary.sort_values(by=["mapped_tasks", "validated_tasks"], ascending=False)

# -----------------------------
# Core Logic
# -----------------------------

async def filter_contributions_by_date(project_id, from_date, to_date, progress_bar, stats_container):
    async with aiohttp.ClientSession() as session:
        tasks_data = await fetch_project_tasks(session, project_id)
        if not tasks_data:
            return None
        
        task_ids = [feature['properties']['taskId'] for feature in tasks_data['features']]
        total_tasks = len(task_ids)
        
        stats_container.metric("Total Tasks Found", total_tasks)
        
        contributions = []
        start_time = time.time()
        
        semaphore = asyncio.Semaphore(10)
        
        async def process_task(task_id, index):
            async with semaphore:
                progress_bar.progress((index + 1) / total_tasks)
                task_details = await fetch_task_details(session, project_id, task_id)

                if task_details and task_details.get('taskHistory'):
                    task_contribs = []
                    for event in task_details['taskHistory']:
                        event_time = parse_datetime(event['actionDate'])
                        if event_time:
                            event_date = event_time.date()
                            if from_date <= event_date <= to_date:
                                task_contribs.append({
                                    'taskId': task_details['taskId'],
                                    'projectId': task_details['projectId'],
                                    'action': event.get('action', ''),
                                    'actionBy': event.get('actionBy', ''),
                                    'actionDate': event['actionDate'],
                                    'taskStatus': task_details.get('taskStatus', '')
                                })
                    return task_contribs if task_contribs else None
                return None
        
        tasks = await asyncio.gather(*[process_task(task_id, i) for i, task_id in enumerate(task_ids)])
        for t in tasks:
            if t:
                contributions.extend(t)
        
        elapsed_time = time.time() - start_time
        stats_container.metric("Fetch Time", f"{elapsed_time:.1f}s")
        stats_container.metric("Total Contributions", len(contributions))
        
        return contributions

# -----------------------------
# Streamlit UI
# -----------------------------

def main():
    st.title("TM Project Contributions Extractor")
    
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    
    with col1:
        project_id = st.text_input("Project ID", value="24229")
    
    with col2:
        from_date = st.date_input("From Date", value=date(2025, 8, 1))
    
    with col3:
        to_date = st.date_input("To Date", value=date.today())
    
    with col4:
        st.write("")
        st.write("")
        filter_button = st.button("Get Contributions", use_container_width=True)
    
    stats_col1, stats_col2, stats_col3 = st.columns(3)
    
    if filter_button:
        try:
            project_id_int = int(project_id)
        except ValueError:
            st.error("Project ID must be a number")
            return
            
        if from_date > to_date:
            st.error("From date cannot be later than To date")
            return
        
        st.info(f"Fetching contributions for project {project_id_int} from {from_date} to {to_date}")
        
        progress_bar = st.progress(0)
        
        loop = asyncio.get_event_loop()
        contributions = loop.run_until_complete(
            filter_contributions_by_date(project_id_int, from_date, to_date, progress_bar, stats_col1)
        )
        progress_bar.empty()
        
        if contributions:
            df = pd.DataFrame(contributions)
            df['actionDate'] = pd.to_datetime(df['actionDate'], errors='coerce')
            
            earliest = df['actionDate'].min()
            latest = df['actionDate'].max()

            with stats_col2:
                st.metric("Unique Contributors", df['actionBy'].nunique())
                st.metric("Unique Tasks", df['taskId'].nunique())
            
            with stats_col3:
                if pd.notna(earliest):
                    st.write("Earliest Action", earliest.strftime("%Y-%m-%d %H:%M"))
                if pd.notna(latest):
                    st.write("Latest Action", latest.strftime("%Y-%m-%d %H:%M"))

            # -----------------------------
            # All Contributions Table
            # -----------------------------
            st.subheader("All Contributions")
            st.dataframe(df, use_container_width=True)

            # -----------------------------
            # Per-User Summary
            # -----------------------------
            st.subheader("Per-User Summary (Overall)")
            summary = df.groupby("actionBy").agg(
                contributions=("actionBy", "count"),
                tasks=("taskId", "nunique"),
                first_action=("actionDate", "min"),
                last_action=("actionDate", "max")
            ).reset_index().sort_values(by="contributions", ascending=False)
            st.dataframe(summary, use_container_width=True)

            # -----------------------------
            # Per-User + Per-Action Summary
            # -----------------------------
            st.subheader("Per-User Action Breakdown")
            user_action_summary = df.groupby(["actionBy", "action"]).size().reset_index(name="count")
            st.dataframe(user_action_summary, use_container_width=True)

            # -----------------------------
            # Mapping & Validation Summary (your refined rules)
            # -----------------------------
            st.subheader("Mapping & Validation Summary (Custom Rules)")
            mapval_summary = compute_mapval_summary(df)
            st.dataframe(mapval_summary, use_container_width=True)

            # -----------------------------
            # Bar Chart of Contributions
            # -----------------------------
            st.subheader("Top Contributors (by # of contributions)")
            chart_data = summary.rename(columns={"contributions": "count"})
            chart = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X("count:Q", title="Contributions"),
                y=alt.Y("actionBy:N", sort='-x', title="User"),
                tooltip=["actionBy", "count", "tasks"]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)

            # -----------------------------
            # Download buttons
            # -----------------------------
            csv_all = df.to_csv(index=False)
            csv_summary = summary.to_csv(index=False)
            csv_user_action = user_action_summary.to_csv(index=False)
            csv_mapval = mapval_summary.to_csv(index=False)

            st.download_button(
                label="Download All Contributions CSV",
                data=csv_all,
                file_name=f"contributions_project_{project_id}_{from_date}_to_{to_date}.csv",
                mime="text/csv"
            )

            st.download_button(
                label="Download Per-User Summary CSV",
                data=csv_summary,
                file_name=f"user_summary_project_{project_id}_{from_date}_to_{to_date}.csv",
                mime="text/csv"
            )

            st.download_button(
                label="Download User-Action Breakdown CSV",
                data=csv_user_action,
                file_name=f"user_action_breakdown_project_{project_id}_{from_date}_to_{to_date}.csv",
                mime="text/csv"
            )

            st.download_button(
                label="Download Mapping & Validation Summary CSV",
                data=csv_mapval,
                file_name=f"user_mapval_summary_project_{project_id}_{from_date}_to_{to_date}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No contributions found in the given date range")

if __name__ == "__main__":
    print("✅ Streamlit app started")
    main()
