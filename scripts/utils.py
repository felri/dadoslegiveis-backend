def normalize_expenses(expense_data):
    # Find the maximum and minimum expenses
    expenses = [x[1] for x in expense_data]
    max_expense = max(expenses)
    min_expense = min(expenses)

    # Normalize the expenses and store in a dictionary
    normalized_expenses = {}
    for state, expense in expense_data:
        normalized_expense = (expense - min_expense) / (max_expense - min_expense)
        normalized_expenses[state] = {
            "expense": expense,
            "normalized_expense": normalized_expense,
        }

    return normalized_expenses
