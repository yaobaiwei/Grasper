/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#include "console_util.hpp"
#include <fstream>

using namespace std;

int ConsoleUtil::Getch() {
    struct termios term, ori_term;
    int c = 0;
    tcgetattr(0, &ori_term);
    memcpy(&term, &ori_term, sizeof(term));
    term.c_lflag &= ~(ICANON | ECHO);
    term.c_cc[VMIN] = 1;
    term.c_cc[VTIME] = 0;
    tcsetattr(0, TCSANOW, &term);
    c = getchar();
    tcsetattr(0, TCSANOW, &ori_term);

    return c;
}

int ConsoleUtil::KBHit() {
    struct termios term, ori_term;
    int c = 0;

    tcgetattr(0, &ori_term);
    memcpy(&term, &ori_term, sizeof(term));
    term.c_lflag &= ~(ICANON | ECHO);
    term.c_cc[VMIN] = 0;
    term.c_cc[VTIME] = 1;
    tcsetattr(0, TCSANOW, &term);
    c = getchar();
    tcsetattr(0, TCSANOW, &ori_term);
    if (c != -1) ungetc(c, stdin);
    return ((c != -1) ? 1 : 0);
}

int ConsoleUtil::KBEsc() {
    int c;

    if (!KBHit()) return KEY_ESCAPE;
    c = Getch();
    if (c == '[') {
        switch (Getch()) {
            case 'A':
                c = KEY_UP;
                break;
            case 'B':
                c = KEY_DOWN;
                break;
            case 'C':
                c = KEY_RIGHT;
                break;
            case 'D':
                c = KEY_LEFT;
                break;
            case 49:
                c = KEY_HOME;
                Getch();
                break;
            case 52:
                c = KEY_END;
                Getch();
                break;
            case 51:
                c = KEY_DELETE;
                Getch();
                break;
            default:
                c = 0;
                break;
        }
    } else {
        c = 0;
    }

    if (c == 0) while (KBHit()) Getch();
    return c;
}

int ConsoleUtil::GetKey() {
    int c;

    c = Getch();
    return (c == KEY_ESCAPE) ? KBEsc() : c;
}

void ConsoleUtil::SetColor(out_colors cl) {
    switch (cl) {
    case K_RED:
        fputs("\x1B[1;31m", stdout);
        break;
    case K_GREEN:
        fputs("\x1B[1;32m", stdout);
        break;
    case K_BLUE:
        fputs("\x1B[1;34m", stdout);
        break;
    case K_YELLOW:
        fputs("\x1B[1;33m", stdout);
        break;
    case K_CYAN:
        fputs("\x1B[1;36m", stdout);
        break;
    case K_MAGENTA:
        fputs("\x1B[1;35m", stdout);
        break;
    case K_WHITE:
        fputs("\x1B[1;37m", stdout);
        break;
    default:
        break;
    }
}

void ConsoleUtil::ResetColor() {
    fputs("\x1B[0m", stdout);
}

void ConsoleUtil::LoadHistory() {
    // map to the actually line
    int actual_line_no = (cur_line_no_ - cur_roll_no_ + BUFFER_LINE) % BUFFER_LINE;

    // fetch the line into the buffer
    memcpy((void*)buffer_, (void*)line_bfs_[actual_line_no], BUFFER_SIZE);
    cur_line_len_ = line_length_[actual_line_no];
    cur_line_pos_ = cur_line_len_;

    SimpleRefreshLine();
}

void ConsoleUtil::OnKeyUp() {
    // printf("\nvoid ConsoleUtil::OnKeyUp()\n");
    int max_key = BUFFER_LINE - 1;

    // when the cur_line_no_ is 98 (and BUFFER_LINE == 100), then 98 history slots are available
    if (cur_line_no_ < BUFFER_LINE) {
        max_key = cur_line_no_;
    }

    if (cur_roll_no_ == max_key) {
        return;  // do nothing, for that cannot roll up
    }

    if (cur_roll_no_ == 0) {
        // buffer the editing line into the line_bfs_
        memcpy((void*)line_bfs_[cur_line_ptr_no_], (void*)buffer_, BUFFER_SIZE);
        // printf("memcpy(line_bfs_[%d], buffer_)\n", cur_line_no_ % BUFFER_LINE);
        line_length_[cur_line_ptr_no_] = cur_line_len_;
    }

    // increase the roll no
    cur_roll_no_++;

    LoadHistory();
}

void ConsoleUtil::OnKeyDown() {
    // printf("\nvoid ConsoleUtil::OnKeyDown()\n");

    // if cur_roll_no_ becomes 0
    if (cur_roll_no_ == 0)
        return;

    cur_roll_no_--;

    LoadHistory();
}

void ConsoleUtil::MoveCursorLeft(int len) {
    if (len > 0)
        printf("\033[%dD", len);
}

void ConsoleUtil::MoveCursorRight(int len) {
    if (len > 0)
        printf("\033[%dC", len);
}

void ConsoleUtil::OnKeyLeft() {
    // printf("\nvoid ConsoleUtil::OnKeyLeft()\n");
    if (cur_line_pos_ > 0) {
        MoveCursorLeft(1);
        cur_line_pos_--;
    }
}

void ConsoleUtil::OnKeyRight() {
    // printf("\nvoid ConsoleUtil::OnKeyRight()\n");
    if (cur_line_pos_ < cur_line_len_) {
        MoveCursorRight(1);
        cur_line_pos_++;
    }
}

string ConsoleUtil::FetchConsoleResult() {
    // generate result
    string s(buffer_);

    bool copy_into_history_ = true;

    if (history_append_dedup_) {
        // if this line is the same with the last line, the buffer won't be written into the history
        // of course, need to check if there is a history line
        if (cur_line_no_ != 0 && BUFFER_LINE != 0) {
            // load the history line
            int history_line_pos = (cur_line_no_ - 1) % BUFFER_LINE;

            if (line_bfs_[history_line_pos] == s)
                copy_into_history_ = false;
        }
    }

    if (copy_into_history_) {
        // copy into the history and increase the seq
        // copy into the history
        memcpy((void*)line_bfs_[cur_line_ptr_no_], (void*)buffer_, BUFFER_SIZE);
        line_length_[cur_line_ptr_no_] = cur_line_len_;
        // printf("memcpy(line_bfs_[%d], buffer_)\n", cur_line_no_);
        // increase the seq
        cur_line_no_++;
        cur_line_ptr_no_ = cur_line_no_ % BUFFER_LINE;
    }

    // disable the buffer
    memset(buffer_, 0, BUFFER_SIZE);

    return s;
}

void ConsoleUtil::SimpleRefreshLine() {
    cout << '\r';  // this moves the cursor to the head of the line
    printf("%c[2K", 27);  // this clears the line
    cout << line_head_;
    printf("%s", buffer_);
}

void ConsoleUtil::OnPrintableKey(int key) {
    // do nothing if too long
    if (cur_line_len_ == BUFFER_SIZE)
        return;

    if (cur_line_pos_ == cur_line_len_) {
        // native print and append
        printf("%c", key);
        sprintf(&buffer_[cur_line_len_++], "%c", key);
        cur_line_pos_++;
    } else {
        // clear the line, print the content, then move back the cursor
        int len_to_backup = cur_line_len_ - cur_line_pos_;
        memcpy((void*)tmp_buffer_, (void*)(&buffer_[cur_line_pos_]), len_to_backup);
        buffer_[cur_line_pos_] = key;
        memcpy((void*)(&buffer_[cur_line_pos_ + 1]), (void*)tmp_buffer_, len_to_backup);
        SimpleRefreshLine();

        MoveCursorLeft(len_to_backup);
        cur_line_pos_++;
        cur_line_len_++;
    }
}

void ConsoleUtil::OnKeyDelete() {
    // do not move the cursor
    if (cur_line_pos_ == cur_line_len_ || cur_line_len_ == 0)
        return;

    if (cur_line_pos_ == cur_line_len_ - 1) {
        // simply erase the last character
        buffer_[cur_line_len_ - 1] = 0;
        cur_line_len_--;
        SimpleRefreshLine();
    } else {
        int len_to_backup = cur_line_len_ - cur_line_pos_ - 1;
        memcpy((void*)tmp_buffer_, (void*)(&buffer_[cur_line_pos_ + 1]), len_to_backup);
        memcpy((void*)(&buffer_[cur_line_pos_]), (void*)tmp_buffer_, len_to_backup);
        buffer_[cur_line_len_ - 1] = 0;
        SimpleRefreshLine();

        MoveCursorLeft(len_to_backup);
        cur_line_len_--;
    }
}

void ConsoleUtil::OnKeyBackspace() {
    if (cur_line_pos_ == cur_line_len_) {
        // cursor at the end of line
        if (cur_line_len_ > 0) {
            buffer_[--cur_line_len_] = 0;
            cur_line_pos_--;
            SimpleRefreshLine();
        }
    } else {
        if (cur_line_pos_ != 0) {
            int len_to_backup = cur_line_len_ - cur_line_pos_;
            memcpy((void*)tmp_buffer_, (void*)(&buffer_[cur_line_pos_]), len_to_backup);
            memcpy((void*)(&buffer_[cur_line_pos_ - 1]), (void*)tmp_buffer_, len_to_backup);
            buffer_[cur_line_len_ - 1] = 0;
            SimpleRefreshLine();

            MoveCursorLeft(len_to_backup);
            cur_line_pos_--;
            cur_line_len_--;
        }
    }
}

string ConsoleUtil::TryConsoleInput(string line_head) {
    cur_roll_no_ = 0;
    cur_line_len_ = 0;
    cur_line_pos_ = 0;

    line_head_ = line_head;
    SimpleRefreshLine();

    int c, i = 0;

    while (true) {
        int key = GetKey();

        // cout<<"<< "<<key<<" >>"<<endl;
        // continue;

        if (isprint(key)) {
            OnPrintableKey(key);
        } else {
            // The coding philosophy of The Scroll Of Taiwu.
            if (key == KEY_RIGHT) {
                OnKeyRight();
            } else if (key == KEY_LEFT) {
                OnKeyLeft();
            } else if (key == KEY_UP) {
                OnKeyUp();
            } else if (key == KEY_DOWN) {
                OnKeyDown();
            } else if (key == KEY_ENTER) {
                // if the line is empty, just do nothing.
                if (cur_line_len_ == 0)
                    continue;

                return FetchConsoleResult();
            } else if (key == KEY_BACKSPACE || key == KEY_DIRECT_BACKSPACE) {  // delete to the left of current cursor
                OnKeyBackspace();
            } else if (key == KEY_HOME) {
                MoveCursorLeft(cur_line_pos_);
                cur_line_pos_ = 0;
            } else if (key == KEY_END) {
                MoveCursorRight(cur_line_len_ - cur_line_pos_);
                cur_line_pos_ = cur_line_len_;
            } else if (key == KEY_DELETE) {  // delete current cursor position
                OnKeyDelete();
            }
        }
    }

    return string(buffer_);
}

void ConsoleUtil::SetConsoleHistory(string path) {
    ifstream of(path);

    // the current buffer
    // and the line pointed by
    if (of.is_open()) {
        char buffer[BUFFER_SIZE];
        while (of.getline(buffer, 20480)) {
            int len = strlen(buffer);
            if (len == 0)
                break;

            memcpy(line_bfs_[cur_line_ptr_no_], buffer, len);
            line_bfs_[cur_line_ptr_no_][len + 1] = 0;
            line_length_[cur_line_ptr_no_] = len;

            cur_line_no_++;
            cur_line_ptr_no_ = cur_line_no_ % BUFFER_LINE;
        }

        of.close();
    }
}

void ConsoleUtil::WriteConsoleHistory(string path) {
    int line_to_write = BUFFER_LINE - 1;

    if (cur_line_no_ < BUFFER_LINE - 1)
        line_to_write = cur_line_no_;

    if (cur_line_no_ == 0)
        return;

    ofstream of(path);

    for (int i = 0; i < line_to_write; i++) {
        int fake_iter = line_to_write - 1 - i;
        of << string(line_bfs_[(cur_line_no_ - fake_iter - 1) % BUFFER_LINE]) << endl;
    }

    of.close();
}

void ConsoleUtil::SetOnQuitWrite(string path) {
    if (path.size() > 0)
        on_quit_write_ = true;
    on_quit_write_path_ = path;
}
