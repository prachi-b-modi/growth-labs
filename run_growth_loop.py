#!/usr/bin/env python3
"""Run the complete Growth Feedback Loop workflow"""

from my_toolkit.tools.bright_data_tool import bright_data_scrape
from my_toolkit.tools.senso_tool import senso_sentiment_intent
from my_toolkit.tools.experiment_designer import design_experiment
from my_toolkit.tools.mixpanel_eval_tool import mixpanel_evaluate_experiment
from my_toolkit.tools.decision_tool import decide_experiment
from my_toolkit.tools.slack_tool import slack_notify
import json

def run_growth_feedback_loop(trigger_event="PAGE_VIEW_DROP"):
    print(f"🚀 Starting Growth Feedback Loop - Trigger: {trigger_event}")
    
    # Step 1: Scrape competitor data
    print("\n📊 Step 1: Scraping competitor data...")
    scrape_result = bright_data_scrape("competitor analysis onboarding retention")
    print(f"Found {len(scrape_result['competitors'])} competitors")
    
    # Step 2: Analyze sentiment from feedback
    print("\n🧠 Step 2: Analyzing sentiment and intent...")
    feedback_texts = [snippet["text"] for snippet in scrape_result["findings"]["feedback_snippets"]]
    sentiment_result = senso_sentiment_intent(feedback_texts)
    print(f"Identified {len(sentiment_result['top_pain_points'])} pain points")
    
    # Step 3: Design experiment
    print("\n🔬 Step 3: Designing experiment...")
    experiment_result = design_experiment(
        metric_primary="retention_d7",
        pain_points=sentiment_result["top_pain_points"],
        competitor_edge=sentiment_result["competitor_edge"]
    )
    exp_id = experiment_result["experiment"]["id"]
    print(f"Created experiment: {exp_id}")
    
    # Step 4: Evaluate experiment (simulate)
    print("\n📈 Step 4: Evaluating experiment results...")
    evaluation_result = mixpanel_evaluate_experiment(exp_id)
    lift = evaluation_result["lift"]["retention_d7_abs"]
    p_value = evaluation_result["p_value"]
    print(f"Results: +{lift*100:.1f}% lift, p={p_value}")
    
    # Step 5: Make decision
    print("\n🎯 Step 5: Making decision...")
    decision_result = decide_experiment(evaluation_result)
    decision = decision_result["decision"]
    print(f"Decision: {decision}")
    
    # Step 6: Notify team
    print("\n📢 Step 6: Notifying team...")
    message = f"🚀 Growth Loop Complete!\n" \
              f"Experiment: {exp_id}\n" \
              f"Result: {decision_result['rationale']}\n" \
              f"Decision: {decision}\n" \
              f"Next: {decision_result['next_actions'][0]}"
    
    notification_result = slack_notify(message)
    print(f"Notification status: {notification_result['status']}")
    
    print("\n✅ Growth Feedback Loop completed successfully!")
    return {
        "trigger": trigger_event,
        "experiment_id": exp_id,
        "decision": decision,
        "lift": lift,
        "p_value": p_value
    }

if __name__ == "__main__":
    result = run_growth_feedback_loop()
    print(f"\n📋 Summary: {json.dumps(result, indent=2)}")