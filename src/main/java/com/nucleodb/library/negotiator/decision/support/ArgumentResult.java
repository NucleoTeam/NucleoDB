package com.nucleodb.library.negotiator.decision.support;

public class ArgumentResult{
  Object resultObject;

  public ArgumentResult(Object resultObject) {
    this.resultObject = resultObject;
  }

  public Object getResultObject() {
    return resultObject;
  }

  public void setResultObject(Object resultObject) {
    this.resultObject = resultObject;
  }
}