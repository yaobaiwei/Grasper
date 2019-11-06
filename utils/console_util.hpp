/* Copyright 2019 Husky Data Lab, CUHK

Authors: Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdio>
#include <string>
#include <cstdlib>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <list>
#include <memory.h>
#include <signal.h>

// TODO(minor): colorful console
#define BLUE COLOR_BLUE
#define RED COLOR_RED
#define WHITE COLOR_WHITE
#define BLACK COLOR_BLACK
#define MAGENTA COLOR_MAGENTA
#define CYAN COLOR_CYAN
#define GREEN COLOR_GREEN
#define YELLOW COLOR_YELLOW
#define LIGHTBLUE COLOR_BLUE
#define LIGHTRED COLOR_RED
#define LIGHTGREEN COLOR_GREEN
#define _cu_get_color_pair(f, b) ((f)|((b) << 3))

// please do not press Esc button, which will causes bug.
class ConsoleUtil {
 private:
    // inner record of the input seq
    // non empty ENTER will increase this by 1
    int cur_line_no_ = 0;
    int cur_line_ptr_no_ = 0;  // always == cur_line_no_ % BUFFER_LINE
    int cur_line_len_;
    int cur_line_pos_;  // the cursor position. from 0 to cur_line_len

    int cur_roll_no_;  // the distance of buffered line

    static const int BUFFER_SIZE = 20480;  // 20KB
    static const int BUFFER_LINE = 100;  // 99 line for history, 1 line for current

    char line_bfs_[BUFFER_LINE][BUFFER_SIZE];
    int line_length_[BUFFER_LINE];
    char buffer_[BUFFER_SIZE];  // the actual line being edited
    char tmp_buffer_[BUFFER_SIZE];  // tmp

    // char** line_bfs_;

    std::list<int> line_identifer_;
    std::list<int>::iterator cur_using_buffer_line_;  // this will be set to 0 at the start of each query

    // char buffer_;

    termios ori_term_attr_;
    ConsoleUtil(const ConsoleUtil&);
    ConsoleUtil& operator=(const ConsoleUtil&);
    ~ConsoleUtil() {
        // printf("ConsoleUtil::~ConsoleUtil()\n");
        // fflush(stdout);
        tcsetattr(STDIN_FILENO, TCSANOW, &ori_term_attr_);
        WriteConsoleHistory(on_quit_write_path_);
    }
    ConsoleUtil() {
        // printf("ConsoleUtil::ConsoleUtil()\n");
        memset(line_length_, 0, sizeof(int) * BUFFER_LINE);
        tcgetattr(STDIN_FILENO, &ori_term_attr_);
        ori_term_attr_.c_lflag |= ICANON | ECHO;

        // signal(SIGINT, (__sighandler_t {aka void (*)(int)})ConsoleUtil::signal_ctrlc);
        signal(SIGINT, ConsoleUtil::signal_ctrlc);
        // printf("overwriten SIGINT (ctrl + c) with exit(0)\n");
    }

    static void signal_ctrlc(int sig) {
        exit(0);
    }

    int Getch();
    int GetKey();
    int KBHit();
    int KBEsc();  // 27

    void SimpleRefreshLine();
    void MoveCursorLeft(int len);
    void MoveCursorRight(int len);
    void LoadHistory();

    void OnKeyUp();
    void OnKeyDown();
    void OnKeyLeft();
    void OnKeyRight();
    void OnKeyBackspace();
    void OnKeyDelete();

    void UpdateCursorPosition();
    void OnPrintableKey(int key);

    std::string FetchConsoleResult();

    std::string line_head_;

    enum KBESC {
        KEY_ESCAPE    = 0x001b,
        KEY_ENTER     = 0x000a,
        KEY_UP        = 0x0105,
        KEY_DOWN      = 0x0106,
        KEY_LEFT      = 0x0107,
        KEY_RIGHT     = 0x0108,
        KEY_BACKSPACE = 0x0008,
        KEY_HOME      = 0x0109,
        KEY_END       = 0x0110,
        KEY_DELETE    = 0x0111,
        KEY_DIRECT_BACKSPACE = 0x07F
    };

    bool on_quit_write_ = false;
    std::string on_quit_write_path_;

    bool history_append_dedup_ = true;

 public:
    static ConsoleUtil* GetInstance() {
        static ConsoleUtil console_single_instance;
        return &console_single_instance;
    }

    std::string TryConsoleInput(std::string line_head = "");

    enum out_colors { K_RED, K_GREEN, K_BLUE, K_YELLOW, K_CYAN, K_MAGENTA, K_WHITE, K_NONE };
    void SetColor(out_colors);
    void ResetColor();

    void SetConsoleHistory(std::string path);
    void WriteConsoleHistory(std::string path);
    void SetHistoryDedup(bool on = true) { history_append_dedup_ = on; }

    // if the process of the program is not controllable
    // can write history on the destructor
    void SetOnQuitWrite(std::string path);
};

