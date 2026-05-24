package org.example.cquirrel.result;

import java.io.Serializable;
import java.util.Objects;

public class AnswerDelta implements Serializable {

    private final JoinAnswer answer;
    private final int multiplicityDelta;

    public AnswerDelta(JoinAnswer answer, int multiplicityDelta) {
        this.answer = answer;
        this.multiplicityDelta = multiplicityDelta;
    }

    public JoinAnswer getAnswer() {
        return answer;
    }

    public int getMultiplicityDelta() {
        return multiplicityDelta;
    }

    @Override
    public String toString() {
        String sign = multiplicityDelta > 0 ? "+" : "";
        return sign + multiplicityDelta + " " + answer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AnswerDelta)) {
            return false;
        }
        AnswerDelta that = (AnswerDelta) o;
        return multiplicityDelta == that.multiplicityDelta
                && Objects.equals(answer, that.answer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(answer, multiplicityDelta);
    }
}
